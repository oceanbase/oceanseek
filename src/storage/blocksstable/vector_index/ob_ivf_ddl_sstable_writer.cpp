/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_ddl_sstable_writer.h"

#include "lib/utility/ob_sort.h"
#include "lib/container/ob_se_array.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/vector_index/quantization/ob_quantization_encoder.h"
#include "storage/ddl/ob_tablet_slice_writer.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace share;
using namespace storage;

namespace
{
static const int64_t IVF_DDL_QUANTIZE_BATCH_SIZE = 4096;

// Writers are private to an output call. Return them on both success and
// failure; a completed slice must not retain its buffers until the DDL ends.
class ObIVFDDLWriterGuard
{
public:
  ObIVFDDLWriterGuard(ObIVFDDLRawWriterProvider *provider, ObITabletSliceWriter *&writer)
    : provider_(provider), writer_(writer) {}
  ~ObIVFDDLWriterGuard()
  {
    if (nullptr != provider_ && nullptr != writer_) {
      provider_->destroy_writer(writer_);
      writer_ = nullptr;
    }
  }
private:
  ObIVFDDLRawWriterProvider *provider_;
  ObITabletSliceWriter *&writer_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFDDLWriterGuard);
};
}

struct ObIVFPostingShard
{
  ObIVFPostingShard() : lock_(ObLatchIds::OB_IVF_BUILD_HELPER_LOCK), begin_(0), end_(0),
      accepted_(0), written_(0), started_(false), finished_(false) {}
  ObIVFSorter sorter_;
  lib::ObMutex lock_;
  int64_t begin_, end_, accepted_, written_;
  bool started_, finished_;
};

ObIVFDDLSSTableWriter::~ObIVFDDLSSTableWriter()
{
  for (auto *&shard : posting_shards_) {
    if (shard != nullptr) {
      shard->~ObIVFPostingShard();
      allocator_.free(shard);
      shard = nullptr;
    }
  }
}

int ObIVFDDLSSTableWriter::prepare_loaded_output(int64_t center_count, int64_t parallelism)
{
  int ret = OB_SUCCESS;
  const int64_t count = MIN(MIN(center_count, parallelism), ObIVFMergeProcessor::MAX_POSTING_TASK_COUNT);
  if (!is_inited_ || center_count <= 0 || parallelism <= 0
      || center_count >= INT64_MAX / ObIVFMergeProcessor::MAX_POSTING_TASK_COUNT
      || output_row_count_ != 0 || quantization_finished_ || posting_shards_[0] != nullptr) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid IVF posting output preparation", K(ret), K(center_count), K(parallelism));
  } else if (!leaf_posting_counts_.empty()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] leaf posting counts already installed before loaded output", K(ret));
  } else if (OB_FAIL(leaf_posting_counts_.prepare_allocate(center_count))) {
    LOG_WARN("[DISK_BBQ] failed to allocate loaded leaf posting counts", K(ret), K(center_count));
  } else {
    MEMSET(leaf_posting_counts_.get_data(), 0, center_count * sizeof(uint32_t));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    void *memory = allocator_.alloc(sizeof(ObIVFPostingShard));
    if (memory == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate IVF posting shard", K(ret), K(i), K(count));
    } else {
      auto &shard = *(posting_shards_[i] = new (memory) ObIVFPostingShard());
      shard.begin_ = (center_count * i + count - 1) / count;
      shard.end_ = (center_count * (i + 1) + count - 1) / count;
      if (OB_FAIL(shard.sorter_.init(MTL_ID()))) {
        LOG_WARN("failed to initialize IVF posting sort", K(ret), K(i), K(count));
      }
    }
  }
  if (OB_SUCC(ret)) {
    posting_center_count_ = center_count;
    posting_shard_count_ = count;
    sorter_.reset(); // Centers stream directly; no second global sorting workspace.
    sorter_inited_ = false;
    LOG_INFO("prepared IVF posting output", K(center_count), K(count));
  }
  return ret;
}

int ObIVFDDLSSTableWriter::append_loaded_postings(const ObIArray<ObIVFBuildRow> &rows)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || quantization_finished_ || raw_write_finished_ || posting_shard_count_ <= 0) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid loaded IVF posting writer state", K(ret), K_(is_inited),
        K_(quantization_finished), K_(raw_write_finished), K_(posting_shard_count));
  } else if (OB_FAIL(append_partitioned_rows_(rows))) {
    LOG_WARN("failed to append loaded IVF posting rows", K(ret), "row_count", rows.count());
  }
  return ret;
}

int ObIVFDDLSSTableWriter::append_partitioned_rows_(const ObIArray<ObIVFBuildRow> &rows)
{
  int ret = ATOMIC_LOAD(&loaded_write_error_);
  int64_t counts[ObIVFMergeProcessor::MAX_POSTING_TASK_COUNT] = {};
  int64_t offsets[ObIVFMergeProcessor::MAX_POSTING_TASK_COUNT] = {};
  ObSEArray<int64_t, 16> positions;
  positions.set_attr(ObMemAttr(MTL_ID(), "IVFPostRoute", ObCtxIds::VECTOR_CTX_ID));
  if (rows.count() > 2 * IVF_DDL_QUANTIZE_BATCH_SIZE || posting_shard_count_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF posting batch size", K(ret), "rows", rows.count(), K_(posting_shard_count));
  } else if (OB_SUCC(ret) && OB_FAIL(positions.prepare_allocate(rows.count()))) {
    LOG_WARN("failed to allocate IVF posting routing", K(ret), "rows", rows.count());
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
    const auto &row = rows.at(i);
    if (row.row_type_ != IVF_ROW_TYPE_LOW_PRECISION_VECTOR || row.cid_ < 0
        || row.cid_ >= posting_center_count_ || row.payload_.length() != one_bit_code_size_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid partitioned IVF posting", K(ret), K(i), K(row.cid_), K(row.row_type_));
    } else { ++counts[row.cid_ * posting_shard_count_ / posting_center_count_]; }
  }
  for (int64_t i = 1; i < posting_shard_count_; ++i) { offsets[i] = offsets[i - 1] + counts[i - 1]; }
  for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
    positions.at(offsets[rows.at(i).cid_ * posting_shard_count_ / posting_center_count_]++) = i;
  }
  int64_t begin = 0;
  for (int64_t s = 0; OB_SUCC(ret) && s < posting_shard_count_; ++s) {
    if (counts[s] == 0) { continue; }
    auto &shard = *posting_shards_[s];
    const int64_t waiting = ObTimeUtility::current_time();
    lib::ObMutexGuard guard(shard.lock_);
    const int64_t locked = ObTimeUtility::current_time();
    int64_t appended = 0;
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("failed to lock IVF posting shard", K(ret), K(s));
    } else if (OB_SUCCESS != (ret = ATOMIC_LOAD(&loaded_write_error_))) {
    } else if (shard.started_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("IVF posting shard already sealed", K(ret), K(s));
    }
    for (int64_t i = begin; OB_SUCC(ret) && i < begin + counts[s]; ++i) {
      if (OB_FAIL(shard.sorter_.append_row(rows.at(positions.at(i))))) {
        LOG_WARN("failed to append IVF posting shard", K(ret), K(s), K(i));
      } else {
        ++appended;
        // Every CID belongs to one shard, whose lock also protects this count.
        // Include copied postings and new SOAR assignments in INSERTION builds.
        const int64_t cid = rows.at(positions.at(i)).cid_;
        if (OB_FAIL(ObIvfCentroidPayload::add_child_count(leaf_posting_counts_.at(cid), 1))) {
          LOG_WARN("[DISK_BBQ] loaded leaf posting count overflow", K(ret), K(cid));
        }
      }
    }
    shard.accepted_ += appended;
    ATOMIC_AAF(&one_bit_row_count_, appended);
    ATOMIC_AAF(&output_row_count_, appended);
    if (OB_FAIL(ret)) { (void)ATOMIC_BCAS(&loaded_write_error_, OB_SUCCESS, ret); }
    ATOMIC_AAF(&sorter_wait_us_, locked - waiting);
    ATOMIC_AAF(&sorter_append_us_, ObTimeUtility::current_time() - locked);
    begin += counts[s];
  }
  return ret;
}

int ObIVFDDLSSTableWriter::append_ddl_row_(ObITabletSliceWriter &writer, int64_t type, int64_t cid,
    ObIVFVectorVID vid, const ObString &payload, ObDatumRow &row)
{
  row.row_flag_.reset();
  row.mvcc_row_flag_.reset();
  row.storage_datums_[row_desc_.row_type_col_idx_].set_int(type);
  row.storage_datums_[row_desc_.centroid_id_col_idx_].set_int(cid);
  row.storage_datums_[row_desc_.vector_id_col_idx_].set_uint(vid);
  row.storage_datums_[row_desc_.trans_version_col_idx_].set_int(-snapshot_version_);
  row.storage_datums_[row_desc_.sql_sequence_col_idx_].set_int(0);
  row.storage_datums_[row_desc_.payload_col_idx_].set_string(payload);
  row.count_ = static_cast<int16_t>(row_desc_.payload_col_idx_ + 1);
  row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
  row.mvcc_row_flag_.set_last_multi_version_row(true);
  return writer.append_row(row);
}

int ObIVFDDLSSTableWriter::write_center_range_(const ObIArray<float> &centers,
    int64_t dim, int64_t begin, int64_t end)
{
  int ret = ATOMIC_LOAD(&loaded_write_error_);
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "IVFCenterWrite", ObCtxIds::VECTOR_CTX_ID));
  ObDatumRow row;
  float *rotated = nullptr;
  char *payload = nullptr;
  if (dim <= 0 || dim != vector_dim_ || begin != 0 || end != posting_center_count_
      || end > INT64_MAX / dim || centers.count() != end * dim
      || leaf_posting_counts_.count() != end
      || (ivf_meta_.is_hierarchical() && centroid_layout_ == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF center output range", K(ret), K(dim), K(begin), K(end));
  }
  if (OB_SUCC(ret)) {
    lib::ObMutexGuard guard(sorter_lock_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("failed to claim IVF center writer", K(ret));
    } else if (centers_started_) {
      ret = OB_INIT_TWICE;
      LOG_WARN("IVF centers already started", K(ret));
    } else { centers_started_ = true; }
  }
  if (OB_FAIL(ret)) { return ret; }
  const int64_t bytes = ObIvfCentroidPayload::get_size(dim);
  ObITabletSliceWriter *writer = nullptr;
  ObIVFDDLWriterGuard writer_guard(nullptr == ddl_writer_ ? raw_writer_provider_ : nullptr, writer);
  if (OB_FAIL(row.init(allocator, row_desc_.payload_col_idx_ + 1))) {
    LOG_WARN("failed to initialize IVF center output row", K(ret));
  } else if (nullptr == (payload = static_cast<char *>(allocator.alloc(bytes)))
      || (!input_vectors_are_rotated_
          && nullptr == (rotated = static_cast<float *>(allocator.alloc(dim * sizeof(float)))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF center output buffers", K(ret), K(dim));
  } else if (OB_FAIL(create_center_writer_(writer))) {
    LOG_WARN("failed to create IVF center writer", K(ret));
  }
  auto append_center = [&](int64_t cid, int64_t vid, const float *value, uint32_t child_count) {
    int ret = OB_SUCCESS;
    if (centroid_row_count_ % 4096 == 0 && OB_FAIL(share::dag_yield())) {
      if (ret == OB_CANCELED) {
        LOG_INFO("[DISK_BBQ] DDL centroid output canceled", K(ret), K(cid), K(vid));
      } else {
        LOG_WARN("[DISK_BBQ] failed to yield during DDL centroid output", K(ret), K(cid), K(vid));
      }
    } else if (rotated != nullptr) {
      MEMCPY(rotated, value, dim * sizeof(float));
      if (OB_FAIL(rotator_.rotate(dim, rotated))) {
        LOG_WARN("[DISK_BBQ] failed to rotate DDL output centroid", K(ret), K(cid), K(vid), K(dim));
      }
      value = rotated;
    }
    if (OB_SUCC(ret) && OB_FAIL(ObIvfCentroidPayload::encode(payload, bytes, value, dim, child_count))) {
      LOG_WARN("failed to encode IVF center output", K(ret), K(cid), K(vid));
    } else if (OB_SUCC(ret) && OB_FAIL(append_ddl_row_(*writer, IVF_ROW_TYPE_CENTROID,
        cid, vid, ObString(static_cast<int32_t>(bytes), payload), row))) {
      LOG_WARN("failed to write IVF center output", K(ret), K(cid), K(vid));
    } else if (OB_SUCC(ret)) {
      ++centroid_row_count_;
      ATOMIC_INC(&output_row_count_);
    }
    return ret;
  };
  if (ivf_meta_.is_hierarchical()) {
    // Slice 0 remains sorted: all parents first, then each contiguous child group.
    for (int64_t p = 0; OB_SUCC(ret) && p < centroid_layout_->parent_count(); ++p) {
      ret = append_center(0, p + 1, centroid_layout_->parent_vector(p), centroid_layout_->group_count(p));
    }
    for (int64_t p = 0; OB_SUCC(ret) && p < centroid_layout_->parent_count(); ++p) {
      for (int64_t pos = centroid_layout_->group_begin(p);
           OB_SUCC(ret) && pos < centroid_layout_->group_end(p); ++pos) {
        const int64_t idx = centroid_layout_->member(pos);
        ret = append_center(p + 1, ivf_meta_.leaf_storage_cid(idx),
            &centers.at(idx * dim), leaf_posting_counts_.at(idx));
      }
    }
  } else {
    for (int64_t idx = begin; OB_SUCC(ret) && idx < end; ++idx) {
      ret = append_center(0, ivf_meta_.leaf_storage_cid(idx),
          &centers.at(idx * dim), leaf_posting_counts_.at(idx));
    }
  }
  if (nullptr != writer) {
    const int close_ret = writer->close();
    if (close_ret != OB_SUCCESS) {
      LOG_WARN("failed to close IVF center writer", K(close_ret));
      if (OB_SUCC(ret)) { ret = close_ret; }
    }
    closed_center_row_count_ = writer->get_row_count();
  }
  if (OB_SUCC(ret)) { centers_finished_ = true; }
  else { (void)ATOMIC_BCAS(&loaded_write_error_, OB_SUCCESS, ret); }
  return ret;
}

int ObIVFDDLSSTableWriter::write_posting_range(int64_t task_idx)
{
  int ret = ATOMIC_LOAD(&loaded_write_error_);
  if (task_idx < 0 || task_idx >= posting_shard_count_ || quantization_finished_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF posting writer task", K(ret), K(task_idx), K_(posting_shard_count));
  }
  if (OB_FAIL(ret)) { return ret; }
  auto &shard = *posting_shards_[task_idx];
  {
    lib::ObMutexGuard guard(shard.lock_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("failed to claim IVF posting writer", K(ret), K(task_idx));
    } else if (shard.started_) {
      ret = OB_INIT_TWICE;
      LOG_WARN("IVF posting writer already started", K(ret), K(task_idx));
    } else { shard.started_ = true; }
  }
  if (OB_FAIL(ret)) { return ret; }
  const int64_t started = ObTimeUtility::current_time();
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "IVFPostingWrite", ObCtxIds::VECTOR_CTX_ID));
  ObDatumRow row;
  ObITabletSliceWriter *writer = nullptr;
  ObIVFDDLWriterGuard writer_guard(raw_writer_provider_, writer);
  if (OB_FAIL(shard.sorter_.finish())) {
    LOG_WARN("failed to sort IVF posting shard", K(ret), K(task_idx));
  } else if (OB_FAIL(row.init(allocator, row_desc_.payload_col_idx_ + 1))) {
    LOG_WARN("failed to initialize IVF posting row", K(ret), K(task_idx));
  } else if (OB_FAIL(raw_writer_provider_->create_posting_writer(task_idx,
      ivf_meta_.leaf_storage_cid(shard.begin_), ivf_meta_.leaf_storage_cid(shard.end_), writer))) {
    LOG_WARN("failed to create IVF posting writer", K(ret), K(task_idx));
  } else if (writer == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null IVF posting writer", K(ret), K(task_idx));
  }
  const int64_t sorted = ObTimeUtility::current_time();
  while (OB_SUCC(ret)) {
    const ObIVFSortItem *item = nullptr;
    if (OB_FAIL(shard.sorter_.get_next_item(item))) {
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; break; }
      LOG_WARN("failed to read IVF posting shard", K(ret), K(task_idx));
    } else if (item == nullptr || item->row_type_ != IVF_ROW_TYPE_LOW_PRECISION_VECTOR
        || item->cid_ < shard.begin_ || item->cid_ >= shard.end_
        || item->payload_.length() != one_bit_code_size_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid sorted IVF posting shard", K(ret), K(task_idx), KPC(item));
    } else if (shard.written_ % 4096 == 0 && OB_FAIL(share::dag_yield())) {
      if (ret == OB_CANCELED) {
        LOG_INFO("[DISK_BBQ] DDL posting output canceled", K(ret), K(task_idx), K(shard.written_));
      } else {
        LOG_WARN("[DISK_BBQ] failed to yield during DDL posting output", K(ret), K(task_idx), K(shard.written_));
      }
    } else if (OB_FAIL(append_ddl_row_(*writer, item->row_type_,
        ivf_meta_.leaf_storage_cid(item->cid_), item->vid_, item->payload_, row))) {
      LOG_WARN("failed to write IVF posting shard", K(ret), K(task_idx), K(shard.written_));
    } else { ++shard.written_; }
  }
  const int64_t written = ObTimeUtility::current_time();
  if (writer != nullptr) {
    const int close_ret = writer->close();
    if (close_ret != OB_SUCCESS) {
      LOG_WARN("failed to close IVF posting writer", K(close_ret), K(task_idx));
      if (OB_SUCC(ret)) { ret = close_ret; }
    }
  }
  if (OB_SUCC(ret) && (shard.written_ != shard.accepted_ || writer->get_row_count() != shard.written_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF posting writer row count mismatch", K(ret), K(task_idx), K(shard.written_), K(shard.accepted_));
  }
  if (OB_SUCC(ret)) {
    shard.finished_ = true;
    shard.sorter_.reset();
    LOG_INFO("IVF posting shard output costs", K(task_idx), K(shard.written_),
        "sort_us", sorted - started, "write_us", written - sorted,
        "close_us", ObTimeUtility::current_time() - written);
  } else { (void)ATOMIC_BCAS(&loaded_write_error_, OB_SUCCESS, ret); }
  return ret;
}

int ObIVFDDLSSTableWriter::finish_partitioned_output_(int64_t vector_count,
    int64_t center_count, int64_t posting_count)
{
  int ret = ATOMIC_LOAD(&loaded_write_error_);
  int64_t written = 0;
  const int64_t physical_centers = centroid_layout_ == nullptr
      ? center_count : centroid_layout_->center_row_count();
  if (OB_SUCC(ret) && (quantization_finished_ || !centers_finished_ || vector_count <= 0
      || center_count != posting_center_count_ || centroid_row_count_ != physical_centers
      || posting_count < vector_count || posting_count > INT64_MAX - physical_centers
      || vector_count > INT64_MAX - physical_centers - posting_count
      || one_bit_row_count_ != posting_count || output_row_count_ != physical_centers + posting_count
      || closed_center_row_count_ != physical_centers)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("incomplete IVF partitioned output", K(ret), K(vector_count), K(center_count), K(posting_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < posting_shard_count_; ++i) {
    const auto &shard = *posting_shards_[i];
    if (!shard.finished_ || shard.written_ != shard.accepted_ || shard.written_ > posting_count - written) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("incomplete IVF posting shard", K(ret), K(i), K(shard.finished_), K(shard.written_));
    } else { written += shard.written_; }
  }
  if (OB_SUCC(ret) && written != posting_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF posting output coverage mismatch", K(ret), K(written), K(posting_count));
  }
  if (OB_SUCC(ret)) {
    ivf_meta_.centroid_count_ = center_count;
    ivf_meta_.vector_count_ = vector_count;
    quantization_finished_ = true;
    centroid_layout_ = nullptr;
    leaf_posting_counts_.reset();
    LOG_INFO("IVF DDL loaded quantization costs", K_(quant_encode_us), K_(sorter_wait_us), K_(sorter_append_us));
  }
  return ret;
}

ObIVFDDLSSTableWriter::ObIVFDDLSSTableWriter(
    ObIAllocator &allocator)
  : centroid_layout_(nullptr),
    allocator_(allocator),
    ddl_writer_(nullptr),
    raw_writer_provider_(nullptr),
    posting_shards_(),
    posting_shard_count_(0),
    posting_center_count_(0),
    leaf_posting_counts_(),
    centers_started_(false), centers_finished_(false),
    raw_ranges_(),
    snapshot_version_(0),
    vector_dim_(0),
    one_bit_code_size_(0),
    row_desc_(),
    rotator_(),
    ivf_meta_(),
    sorter_(),
    sorter_tenant_id_(OB_INVALID_TENANT_ID),
    sorter_inited_(false),
    sorter_lock_(ObLatchIds::OB_IVF_BUILD_HELPER_LOCK),
    centroid_row_count_(0),
    closed_center_row_count_(0),
    one_bit_row_count_(0),
    raw_vector_row_count_(0),
    output_row_count_(0),
    loaded_write_error_(OB_SUCCESS), sorter_wait_us_(0), sorter_append_us_(0), quant_encode_us_(0),
    is_inited_(false),
    input_vectors_are_rotated_(false),
    quantization_finished_(false),
    raw_write_finished_(false)
{
  leaf_posting_counts_.set_block_allocator(ObWrapperAllocator(allocator));
}

int ObIVFDDLSSTableWriter::set_leaf_posting_counts(const ObIArray<uint32_t> &counts)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] DDL writer is not initialized for leaf posting counts", K(ret));
  } else if (counts.empty() || (posting_center_count_ > 0 && counts.count() != posting_center_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid DDL leaf posting counts", K(ret),
        "input_count", counts.count(), K_(posting_center_count));
  } else if (quantization_finished_ || centers_started_
      || (posting_shard_count_ == 0 && !leaf_posting_counts_.empty())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] invalid DDL leaf posting count installation state", K(ret),
        K_(is_inited), K_(quantization_finished), K_(centers_started), K_(posting_center_count),
        "input_count", counts.count(), "retained_count", leaf_posting_counts_.count());
  }
  if (OB_SUCC(ret) && posting_shard_count_ > 0) {
    // Loaded writers already counted actual emitted rows under shard locks.
    // At the SOAR barrier, validate the producer's independent accounting.
    for (int64_t cid = 0; OB_SUCC(ret) && cid < counts.count(); ++cid) {
      if (counts.at(cid) != leaf_posting_counts_.at(cid)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("[DISK_BBQ] loaded posting count disagrees with SOAR assignments", K(ret), K(cid),
            "expected", counts.at(cid), "written", leaf_posting_counts_.at(cid));
      }
    }
  } else if (OB_SUCC(ret) && OB_FAIL(leaf_posting_counts_.assign(counts))) {
    LOG_WARN("[DISK_BBQ] failed to retain DDL leaf posting counts", K(ret), "count", counts.count());
  }
  if (OB_SUCC(ret)) { ivf_meta_.centroid_count_ = counts.count(); }
  return ret;
}

int ObIVFDDLSSTableWriter::set_centroid_layout(const ObIvfCentroidLayout &layout)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] DDL writer is not initialized for centroid layout", K(ret));
  } else if (!layout.is_ready() || layout.dimension() != vector_dim_
      || (posting_center_count_ > 0 && layout.leaf_count() != posting_center_count_)
      || (!leaf_posting_counts_.empty() && layout.leaf_count() != leaf_posting_counts_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid DDL centroid layout", K(ret), K_(vector_dim), K_(posting_center_count),
        "layout_ready", layout.is_ready(), "layout_dimension", layout.dimension(),
        "layout_leaf_count", layout.leaf_count(), "retained_count", leaf_posting_counts_.count());
  } else if (centroid_layout_ != nullptr || quantization_finished_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] invalid DDL centroid layout installation state", K(ret),
        K_(is_inited), K_(quantization_finished), KP_(centroid_layout), K_(vector_dim),
        K_(posting_center_count), "layout_ready", layout.is_ready(),
        "layout_dimension", layout.dimension(), "layout_leaf_count", layout.leaf_count(),
        "retained_count", leaf_posting_counts_.count());
  } else {
    centroid_layout_ = &layout;
    layout.set_meta(ivf_meta_);
  }
  return ret;
}

int ObIVFDDLSSTableWriter::init(
    ObITabletSliceWriter &ddl_writer,
    ObIVFDDLRawWriterProvider &raw_writer_provider,
    const int64_t snapshot_version,
    const int64_t vector_dim,
    const ObVectorIndexDistAlgorithm dist_type,
    const bool input_vectors_are_rotated)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF DDL SSTable writer init twice", K(ret));
  } else if (0 != ddl_writer.get_row_count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("IVF DDL sink is not empty", K(ret), "row_count", ddl_writer.get_row_count());
  } else if (OB_FAIL(init(raw_writer_provider, snapshot_version, vector_dim,
                         dist_type, input_vectors_are_rotated))) {
    LOG_WARN("failed to initialize IVF DDL output", K(ret));
  } else {
    ddl_writer_ = &ddl_writer;
  }
  return ret;
}

int ObIVFDDLSSTableWriter::init(
    ObIVFDDLRawWriterProvider &raw_writer_provider,
    const int64_t snapshot_version,
    const int64_t vector_dim,
    const ObVectorIndexDistAlgorithm dist_type,
    const bool input_vectors_are_rotated)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator encoder_allocator(
      ObMemAttr(MTL_ID(), "IVFQuantInit", ObCtxIds::VECTOR_CTX_ID));
  quantization::ObQuantizationParams params = {};
  quantization::ObQuantizationEncoder encoder;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF DDL SSTable writer init twice", K(ret));
  } else if (snapshot_version <= 0 || vector_dim <= 0
      || dist_type < VIDA_L2 || dist_type >= VIDA_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF DDL SSTable writer argument", K(ret),
        K(snapshot_version), K(vector_dim), K(dist_type));
  } else if (vector_dim > INT32_MAX / static_cast<int64_t>(sizeof(float))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("IVF vector dimension is too large", K(ret), K(vector_dim));
  } else if (OB_FAIL(row_desc_.init(IVF_SSTABLE_FORMAT_VERSION))) {
    LOG_WARN("failed to initialize IVF DDL output row descriptor", K(ret));
  } else if (OB_FAIL(quantization::ObFhtKacRotator::generate(
                 allocator_, vector_dim, rotator_))) {
    LOG_WARN("failed to initialize IVF DDL FHT rotator",
        K(ret), K(vector_dim));
  } else {
    params.dim_ = vector_dim;
    params.bits_ = 1;
    params.rotator_ = rotator_;
    if (OB_FAIL(encoder.init(
        encoder_allocator,
        quantization::ObQuantizationType::RABITQ,
        dist_type,
        params))) {
      LOG_WARN("failed to initialize one-bit IVF DDL encoder", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    snapshot_version_ = snapshot_version;
    vector_dim_ = vector_dim;
    input_vectors_are_rotated_ = input_vectors_are_rotated;
    one_bit_code_size_ = encoder.code_size();
    ivf_meta_.reset();
    ivf_meta_.index_type_ = VIAT_DISK_BBQ;
    ivf_meta_.dist_type_ = dist_type;
    ivf_meta_.dim_ = static_cast<int32_t>(vector_dim_);
    ivf_meta_.format_version_ = IVF_SSTABLE_FORMAT_VERSION;
    ivf_meta_.nbits_ = 1;
    ivf_meta_.fht_algo_version_ =
        quantization::ObFhtKacRotator::CURRENT_FHT_ALGO_VERSION;
    if (one_bit_code_size_ <= 0 || !ivf_meta_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid one-bit IVF DDL output metadata",
          K(ret), K_(one_bit_code_size), K_(ivf_meta));
    } else {
      // Output mode is selected after training. Keep the owning tenant, but
      // allocate a global sorter only if that mode actually appends rows.
      sorter_tenant_id_ = MTL_ID();
      raw_writer_provider_ = &raw_writer_provider;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObIVFDDLSSTableWriter::ensure_sorter_inited_()
{
  int ret = OB_SUCCESS;
  if (posting_shard_count_ > 0) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("global sorter is not used for partitioned IVF output", K(ret), K_(posting_shard_count));
  } else if (!sorter_inited_) {
    if (OB_FAIL(sorter_.init(sorter_tenant_id_))) {
      LOG_WARN("failed to initialize IVF DDL physical-row sorter", K(ret), K_(sorter_tenant_id));
    } else {
      sorter_inited_ = true;
    }
  }
  return ret;
}

int ObIVFDDLSSTableWriter::create_center_writer_(ObITabletSliceWriter *&writer)
{
  int ret = OB_SUCCESS;
  writer = nullptr;
  if (!is_inited_ || nullptr == raw_writer_provider_) {
    ret = OB_NOT_INIT;
  } else if (nullptr != ddl_writer_) {
    writer = ddl_writer_;
  } else if (OB_FAIL(raw_writer_provider_->create_center_writer(writer))) {
    LOG_WARN("failed to create IVF center output sink", K(ret));
  }
  if (OB_SUCC(ret) && (nullptr == writer || 0 != writer->get_row_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid IVF center output sink", K(ret), KP(writer));
  }
  return ret;
}

int ObIVFDDLSSTableWriter::append_sort_row_(
    const int64_t row_type,
    const int64_t cid,
    const ObIVFVectorVID vid,
    const ObString &payload)
{
  int ret = OB_SUCCESS;
  ObIVFBuildRow row;
  if (!is_inited_ || raw_write_finished_
      || quantization_finished_
      || cid < 0 || payload.empty()
      || (IVF_ROW_TYPE_CENTROID != row_type
      && IVF_ROW_TYPE_LOW_PRECISION_VECTOR != row_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sorted IVF DDL output row", K(ret), K(row_type), K(cid),
        K(vid), "payload_size", payload.length(), K_(is_inited),
        K_(quantization_finished), K_(raw_write_finished));
  } else {
    row.row_type_ = row_type;
    row.cid_ = cid;
    row.vid_ = vid;
    row.payload_ = payload;
    lib::ObMutexGuard guard(sorter_lock_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("failed to lock IVF DDL physical-row sorter", K(ret), K(row));
    } else if (output_row_count_ == INT64_MAX
        || (IVF_ROW_TYPE_CENTROID == row_type
            && centroid_row_count_ == INT64_MAX)
        || (IVF_ROW_TYPE_LOW_PRECISION_VECTOR == row_type
            && one_bit_row_count_ == INT64_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("IVF DDL sorted output row count overflow", K(ret), K(row_type),
          K(cid), K(vid), K_(output_row_count), K_(centroid_row_count),
          K_(one_bit_row_count));
    } else if (OB_FAIL(ensure_sorter_inited_())) {
      LOG_WARN("failed to prepare IVF DDL physical-row sorter", K(ret));
    } else if (OB_FAIL(sorter_.append_row(row))) {
      LOG_WARN("failed to append IVF DDL physical row to sorter",
          K(ret), K(row));
    } else {
      ++output_row_count_;
      if (IVF_ROW_TYPE_CENTROID == row_type) {
        ++centroid_row_count_;
      } else {
        ++one_bit_row_count_;
      }
    }
  }
  return ret;
}

int ObIVFDDLSSTableWriter::quantize_cluster(
    ObIVFVectorReader &reader,
    const ObIArray<float> &final_centers,
    const int64_t center_count,
    const int64_t cid,
    const ObIArray<ObIVFVectorVID> &primary_member_vids,
    const ObIArray<ObIVFVectorVID> *secondary_member_vids)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator task_allocator(
      ObMemAttr(MTL_ID(), "IVFQuantTask", ObCtxIds::VECTOR_CTX_ID));
  quantization::ObQuantizationParams params = {};
  quantization::ObQuantizationEncoder encoder;
  ObArray<ObIVFVectorVID, ObWrapperAllocator> posting_vids;
  // Both the writer and reader reuse this buffer. Retain its capacity because
  // the task arena only frees old allocations when the entire range finishes.
  ObSEArray<float, 16, ObWrapperAllocator> vectors(
      OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(task_allocator));
  char *one_bit_codes = nullptr;
  const float *center = nullptr;
  float *rotated_center = nullptr;
  float *rotated_vector = nullptr;
  uint32_t posting_count = 0;
  posting_vids.set_block_allocator(ObWrapperAllocator(task_allocator));
  if (OB_FAIL(ObIvfCentroidPayload::add_child_count(posting_count, primary_member_vids.count()))) {
    LOG_WARN("IVF DDL primary posting count overflow", K(ret), K(cid));
  } else if (OB_NOT_NULL(secondary_member_vids)
      && OB_FAIL(ObIvfCentroidPayload::add_child_count(posting_count, secondary_member_vids->count()))) {
    LOG_WARN("IVF DDL posting count overflow", K(ret), K(cid), K(posting_count),
        "secondary_count", secondary_member_vids->count());
  }
  if (OB_FAIL(ret)) {
  } else if (!is_inited_ || quantization_finished_
      || reader.get_dimension() != vector_dim_
      || center_count <= 0 || cid < 0 || cid >= center_count
      || center_count > INT64_MAX / vector_dim_
      || final_centers.count() != center_count * vector_dim_
      || primary_member_vids.empty() || posting_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF DDL cluster quantization argument", K(ret),
        K(center_count), K(cid), K_(vector_dim),
        "reader_dim", reader.get_dimension(),
        "center_value_count", final_centers.count(),
        "primary_count", primary_member_vids.count());
  } else {
    center = &final_centers.at(cid * vector_dim_);
    params.dim_ = vector_dim_;
    params.bits_ = 1;
    params.rotator_ = rotator_;
    if (OB_FAIL(encoder.init(
        task_allocator,
        quantization::ObQuantizationType::RABITQ,
        static_cast<ObVectorIndexDistAlgorithm>(ivf_meta_.dist_type_),
        params))) {
      LOG_WARN("failed to initialize one-bit encoder for IVF DDL cluster",
          K(ret), K(cid));
    } else if (encoder.code_size() != one_bit_code_size_
        || posting_count > INT64_MAX / one_bit_code_size_
        || OB_ISNULL(one_bit_codes = static_cast<char *>(
            task_allocator.alloc_align(
                posting_count * one_bit_code_size_, alignof(float))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate one-bit IVF DDL codes",
          K(ret), K(cid), K(posting_count), K_(one_bit_code_size));
    } else if (OB_ISNULL(rotated_center = static_cast<float *>(
        task_allocator.alloc_align(
            vector_dim_ * sizeof(float), alignof(float))))
        || OB_ISNULL(rotated_vector = static_cast<float *>(
        task_allocator.alloc_align(
            vector_dim_ * sizeof(float), alignof(float))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate IVF DDL rotation buffers",
          K(ret), K(cid), K_(vector_dim));
    } else {
      MEMCPY(rotated_center, center, vector_dim_ * sizeof(float));
      if (!input_vectors_are_rotated_
          && OB_FAIL(rotator_.rotate(vector_dim_, rotated_center))) {
        LOG_WARN("failed to rotate IVF DDL centroid", K(ret), K(cid));
      }
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(posting_vids.reserve(posting_count))) {
    LOG_WARN("failed to reserve IVF DDL posting VIDs",
        K(ret), K(cid), K(posting_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < primary_member_vids.count(); ++i) {
    ret = posting_vids.push_back(primary_member_vids.at(i));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to append primary IVF DDL posting VID", K(ret), K(cid),
          K(i), K(primary_member_vids.at(i)));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && OB_NOT_NULL(secondary_member_vids)
      && i < secondary_member_vids->count(); ++i) {
    ret = posting_vids.push_back(secondary_member_vids->at(i));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to append secondary IVF DDL posting VID", K(ret), K(cid),
          K(i), K(secondary_member_vids->at(i)));
    }
  }
  if (OB_SUCC(ret)) {
    lib::ob_sort(posting_vids.begin(), posting_vids.end());
    for (int64_t i = 1; OB_SUCC(ret) && i < posting_vids.count(); ++i) {
      if (posting_vids.at(i - 1) == posting_vids.at(i)) {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        LOG_WARN("duplicate one-bit posting in IVF DDL cluster",
            K(ret), K(cid), K(posting_vids.at(i)));
      }
    }
  }

  for (int64_t batch_begin = 0;
       OB_SUCC(ret) && batch_begin < posting_vids.count();
       batch_begin += IVF_DDL_QUANTIZE_BATCH_SIZE) {
    const int64_t batch_end = MIN(
        posting_vids.count(), batch_begin + IVF_DDL_QUANTIZE_BATCH_SIZE);
    vectors.reuse();
    if (OB_FAIL(reader.batch_get_by_vid(
        posting_vids, batch_begin, batch_end, vectors))) {
      LOG_WARN("failed to read vectors for one-bit IVF DDL quantization",
          K(ret), K(cid), K(batch_begin), K(batch_end));
    } else if (vectors.count()
        != (batch_end - batch_begin) * vector_dim_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected IVF DDL quantization vector count", K(ret), K(cid),
          K(batch_begin), K(batch_end), "value_count", vectors.count());
    }
    for (int64_t member_idx = batch_begin;
         OB_SUCC(ret) && member_idx < batch_end;
         ++member_idx) {
      const int64_t local_idx = member_idx - batch_begin;
      MEMCPY(rotated_vector, &vectors.at(local_idx * vector_dim_),
          vector_dim_ * sizeof(float));
      if (!input_vectors_are_rotated_
          && OB_FAIL(rotator_.rotate(vector_dim_, rotated_vector))) {
        LOG_WARN("failed to rotate IVF DDL posting vector",
            K(ret), K(cid), K(member_idx));
      } else if (OB_FAIL(encoder.encode(
          rotated_vector, rotated_center,
          {reinterpret_cast<uint8_t *>(one_bit_codes
              + member_idx * one_bit_code_size_),
           one_bit_code_size_}))) {
        LOG_WARN("failed to encode one-bit IVF DDL posting",
            K(ret), K(cid), K(posting_vids.at(member_idx)));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(share::dag_yield())) {
      LOG_WARN("one-bit IVF DDL quantization was interrupted",
          K(ret), K(cid), K(batch_begin), K(batch_end));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < posting_vids.count(); ++i) {
    const ObString code(
        static_cast<int32_t>(one_bit_code_size_),
        one_bit_codes + i * one_bit_code_size_);
    if (OB_FAIL(append_sort_row_(
        IVF_ROW_TYPE_LOW_PRECISION_VECTOR, ivf_meta_.leaf_storage_cid(cid), posting_vids.at(i), code))) {
      LOG_WARN("failed to retain one-bit IVF DDL posting",
          K(ret), K(cid), K(i), K(posting_vids.at(i)));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t bytes = ObIvfCentroidPayload::get_size(vector_dim_);
    char *buf = static_cast<char *>(task_allocator.alloc(bytes));
    if (buf == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate DDL centroid payload", K(ret), K(cid),
          K_(vector_dim), "alloc_size", bytes);
    } else if (OB_FAIL(ObIvfCentroidPayload::encode(buf, bytes, rotated_center, vector_dim_, posting_count))) {
      LOG_WARN("failed to encode IVF DDL center payload", K(ret), K(cid));
    } else {
      const ObString payload(static_cast<int32_t>(bytes), buf);
      const int64_t self = ivf_meta_.leaf_storage_cid(cid);
      if (!ivf_meta_.is_hierarchical()) {
        if (OB_FAIL(append_sort_row_(IVF_ROW_TYPE_CENTROID, 0, self, payload))) {
          LOG_WARN("[DISK_BBQ] failed to retain DDL flat centroid", K(ret), K(cid), K(self));
        }
      } else if (centroid_layout_ == nullptr) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("[DISK_BBQ] missing centroid layout during DDL write", K(ret), K(cid), K(self));
      } else if (OB_FAIL(append_sort_row_(IVF_ROW_TYPE_CENTROID,
          centroid_layout_->primary_parent(cid), self, payload))) {
        LOG_WARN("[DISK_BBQ] failed to retain DDL primary child centroid", K(ret), K(cid), K(self));
      } else if (centroid_layout_->secondary_parent(cid) > 0) {
        if (OB_FAIL(append_sort_row_(IVF_ROW_TYPE_CENTROID,
            centroid_layout_->secondary_parent(cid), self, payload))) {
          LOG_WARN("[DISK_BBQ] failed to retain DDL secondary child centroid", K(ret), K(cid), K(self));
        }
      }
    }
  }
  return ret;
}

int ObIVFDDLSSTableWriter::append_loaded_rows_(const ObIArray<ObIVFBuildRow> &rows)
{
  int ret = ATOMIC_LOAD(&loaded_write_error_);
  for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
    const auto &row = rows.at(i);
    if (!is_inited_ || quantization_finished_ || raw_write_finished_ || row.cid_ < 0 || row.payload_.empty()
        || (row.row_type_ != IVF_ROW_TYPE_CENTROID && row.row_type_ != IVF_ROW_TYPE_LOW_PRECISION_VECTOR)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid loaded IVF output row", K(ret), K(i), K_(is_inited), K_(quantization_finished),
               K_(raw_write_finished), K(row.cid_), K(row.row_type_));
    }
  }
  if (OB_SUCC(ret) && posting_shard_count_ > 0) { return append_partitioned_rows_(rows); }
  if (OB_SUCC(ret)) {
    const int64_t begin = ObTimeUtility::current_time();
    lib::ObMutexGuard guard(sorter_lock_);
    const int64_t locked = ObTimeUtility::current_time();
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("failed to lock loaded IVF output sorter", K(ret), "rows", rows.count());
    } else if (OB_SUCCESS != (ret = ATOMIC_LOAD(&loaded_write_error_))) {
    } else if (rows.count() > INT64_MAX - output_row_count_) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("loaded IVF output row count overflow", K(ret), "rows", rows.count(), K_(output_row_count));
    } else if (!rows.empty() && OB_FAIL(ensure_sorter_inited_())) {
      LOG_WARN("failed to prepare loaded IVF output sorter", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
      const auto &row = rows.at(i);
      if (OB_FAIL(sorter_.append_row(row))) {
        LOG_WARN("failed to append loaded IVF output row", K(ret), K(i), K(row.cid_), K(row.vid_), K(row.row_type_),
                 K_(output_row_count));
      } else {
        ++output_row_count_;
        if (row.row_type_ == IVF_ROW_TYPE_CENTROID) { ++centroid_row_count_; }
        else { ++one_bit_row_count_; }
      }
    }
    if (OB_FAIL(ret)) {
      // A partial physical append cannot be retried. Keep the first error
      // sticky so no later attempt can duplicate an already-appended prefix.
      ATOMIC_STORE(&loaded_write_error_, ret);
    }
    ATOMIC_AAF(&sorter_wait_us_, locked - begin);
    ATOMIC_AAF(&sorter_append_us_, ObTimeUtility::current_time() - locked);
  }
  return ret;
}

int ObIVFDDLSSTableWriter::write_loaded_centers(const ObIArray<float> &centers,
    int64_t dim, int64_t begin, int64_t end)
{
  if (posting_shard_count_ > 0) { return write_center_range_(centers, dim, begin, end); }
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "IVFDDLCenters", ObCtxIds::VECTOR_CTX_ID));
  char *payload = nullptr;
  float *rotated = nullptr;
  if (!is_inited_ || dim != vector_dim_ || dim <= 0 || centers.count() % dim != 0
      || begin < 0 || end <= begin || end > centers.count() / dim
      || leaf_posting_counts_.count() != centers.count() / dim
      || (ivf_meta_.is_hierarchical() && centroid_layout_ == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid loaded IVF center batch", K(ret), K(dim), K(begin), K(end));
  }
  const int64_t bytes = ObIvfCentroidPayload::get_size(dim);
  if (OB_SUCC(ret) && (nullptr == (payload = static_cast<char *>(allocator.alloc(bytes)))
      || (!input_vectors_are_rotated_
          && nullptr == (rotated = static_cast<float *>(allocator.alloc(dim * sizeof(float))))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[DISK_BBQ] failed to allocate loaded DDL centroid buffers", K(ret),
        K(dim), K(begin), K(end), "payload_bytes", bytes,
        "vector_bytes", input_vectors_are_rotated_ ? 0 : dim * sizeof(float), KP(payload), KP(rotated));
  }
  for (int64_t idx = begin; OB_SUCC(ret) && idx < end; ++idx) {
    const float *center = &centers.at(idx * dim);
    if (rotated != nullptr) {
      MEMCPY(rotated, center, dim * sizeof(float));
      ret = rotator_.rotate(dim, rotated);
      center = rotated;
    }
    if (OB_SUCC(ret) && OB_FAIL(ObIvfCentroidPayload::encode(payload, bytes, center,
        dim, leaf_posting_counts_.at(idx)))) {
      LOG_WARN("failed to encode loaded IVF center", K(ret), K(idx));
    } else if (OB_SUCC(ret)) {
      const ObString value(static_cast<int32_t>(bytes), payload);
      const int64_t self = ivf_meta_.leaf_storage_cid(idx);
      if (!ivf_meta_.is_hierarchical()) {
        if (OB_FAIL(append_sort_row_(IVF_ROW_TYPE_CENTROID, 0, self, value))) {
          LOG_WARN("[DISK_BBQ] failed to retain loaded flat centroid", K(ret), K(idx), K(self));
        }
      } else if (OB_FAIL(append_sort_row_(IVF_ROW_TYPE_CENTROID,
          centroid_layout_->primary_parent(idx), self, value))) {
        LOG_WARN("[DISK_BBQ] failed to retain loaded primary child centroid", K(ret), K(idx), K(self));
      } else if (centroid_layout_->secondary_parent(idx) > 0) {
        if (OB_FAIL(append_sort_row_(IVF_ROW_TYPE_CENTROID,
            centroid_layout_->secondary_parent(idx), self, value))) {
          LOG_WARN("[DISK_BBQ] failed to retain loaded secondary child centroid", K(ret), K(idx), K(self));
        }
      }
    }
  }
  return ret;
}

int ObIVFDDLSSTableWriter::quantize_loaded_vectors(const ObIArray<float> &centers, int64_t dim,
    const ObIArray<ObIVFVectorVID> &vids, const ObIArray<float> &vectors,
    const ObIArray<int64_t> &primary, const ObIArray<int64_t> *secondary)
{
  int ret = OB_SUCCESS;
  const int64_t started = ObTimeUtility::current_time();
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "IVFDDLQuant", ObCtxIds::VECTOR_CTX_ID));
  quantization::ObQuantizationParams params = {};
  quantization::ObQuantizationEncoder encoder;
  ObArray<ObIVFBuildRow, ObWrapperAllocator> rows;
  rows.set_block_allocator(ObWrapperAllocator(allocator));
  char *codes = nullptr;
  float *rotated_vector = nullptr, *rotated_center = nullptr;
  const int64_t n = vids.count();
  int64_t posting_count = n;
  if (!is_inited_ || quantization_finished_ || raw_write_finished_ || dim != vector_dim_ || dim <= 0 || n <= 0 ||
      n > INT64_MAX / 2 || n > INT64_MAX / dim || vectors.count() != n * dim || centers.empty() ||
      centers.count() % dim != 0 || primary.count() != n || (secondary != nullptr && secondary->count() != n)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid loaded IVF quantization input", K(ret), K(dim), K(n), "values", vectors.count(), "centers",
             centers.count(), "primary", primary.count(), KP(secondary));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
    const int64_t a1 = primary.at(i), a2 = secondary == nullptr ? -1 : secondary->at(i);
    if (a1 < 0 || a1 >= centers.count() / dim || a2 < -1 || a2 >= centers.count() / dim || a1 == a2) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid loaded IVF quantization assignments", K(ret), K(i), K(a1), K(a2), "center_values",
               centers.count(), K(dim));
    } else if (a2 >= 0) { ++posting_count; }
  }
  if (OB_SUCC(ret)) {
    params.dim_ = dim; params.bits_ = 1; params.rotator_ = rotator_;
    if (OB_FAIL(encoder.init(allocator, quantization::ObQuantizationType::RABITQ,
        static_cast<ObVectorIndexDistAlgorithm>(ivf_meta_.dist_type_), params))) {
      LOG_WARN("failed to initialize loaded IVF quantization encoder", K(ret), K(dim), K(n));
    } else if (encoder.code_size() != one_bit_code_size_ || one_bit_code_size_ <= 0 ||
               posting_count > INT64_MAX / one_bit_code_size_) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("invalid loaded IVF quantization code size", K(ret), "code_size", encoder.code_size(),
               K_(one_bit_code_size), K(posting_count));
    } else if (nullptr == (codes = static_cast<char *>(
                               allocator.alloc_align(posting_count * one_bit_code_size_, alignof(float))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate loaded IVF quantization codes", K(ret), K(posting_count), K_(one_bit_code_size));
    } else if (OB_FAIL(rows.reserve(posting_count))) {
      LOG_WARN("failed to reserve loaded IVF quantization rows", K(ret), K(posting_count));
    } else if (!input_vectors_are_rotated_) {
      rotated_vector = static_cast<float *>(allocator.alloc(dim * sizeof(float)));
      rotated_center = static_cast<float *>(allocator.alloc(dim * sizeof(float)));
      if (rotated_vector == nullptr || rotated_center == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate loaded IVF quantization rotation buffers", K(ret), K(dim));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
    const float *vector = &vectors.at(i * dim);
    if (!input_vectors_are_rotated_) {
      MEMCPY(rotated_vector, vector, dim * sizeof(float));
      if (OB_FAIL(rotator_.rotate(dim, rotated_vector))) {
        LOG_WARN("failed to rotate loaded IVF vector", K(ret), K(i), K(dim));
      }
      vector = rotated_vector;
    }
    const int64_t cids[] = {primary.at(i), secondary == nullptr ? -1 : secondary->at(i)};
    for (int assignment = 0; OB_SUCC(ret) && assignment < 2; ++assignment) {
      const int64_t cid = cids[assignment];
      if (cid < 0) { continue; }
      const float *center = &centers.at(cid * dim);
      if (!input_vectors_are_rotated_) {
        MEMCPY(rotated_center, center, dim * sizeof(float));
        if (OB_FAIL(rotator_.rotate(dim, rotated_center))) {
          LOG_WARN("failed to rotate loaded IVF posting center", K(ret), K(i), K(cid), K(dim));
        }
        center = rotated_center;
      }
      char *code = codes + rows.count() * one_bit_code_size_;
      if (OB_SUCC(ret) && OB_FAIL(encoder.encode(vector, center,
          {reinterpret_cast<uint8_t *>(code), one_bit_code_size_}))) {
        LOG_WARN("failed to encode loaded IVF posting", K(ret), K(i), K(cid), "vid", vids.at(i), K_(one_bit_code_size));
      } else if (OB_SUCC(ret)) {
        ObIVFBuildRow row;
        row.row_type_ = IVF_ROW_TYPE_LOW_PRECISION_VECTOR;
        row.cid_ = posting_shard_count_ > 0 ? cid : ivf_meta_.leaf_storage_cid(cid);
        row.vid_ = vids.at(i);
        row.payload_.assign_ptr(code, one_bit_code_size_);
        if (OB_FAIL(rows.push_back(row))) {
          LOG_WARN("failed to retain loaded IVF output row", K(ret), K(row.cid_), K(row.vid_), K(row.row_type_));
        }
      }
    }
  }
  ATOMIC_AAF(&quant_encode_us_, ObTimeUtility::current_time() - started);
  // Encoding is complete before taking the sorter lock. Only compact codes
  // enter the sorter; full-precision vectors remain task-local.
  if (OB_SUCC(ret) && OB_FAIL(share::dag_yield())) {
  } else if (OB_SUCC(ret)) { ret = append_loaded_rows_(rows); }
  return ret;
}

int ObIVFDDLSSTableWriter::finish_quantization(
    const int64_t vector_count,
    const int64_t center_count,
    const int64_t posting_count)
{
  if (posting_shard_count_ > 0) {
    return finish_partitioned_output_(vector_count, center_count, posting_count);
  }
  int ret = OB_SUCCESS;
  const int64_t physical_centers = centroid_layout_ == nullptr ? center_count : centroid_layout_->center_row_count();
  int64_t expected_row_count = 0;
  if (is_inited_ && !quantization_finished_ && centroid_layout_ != nullptr && centroid_layout_->is_hierarchical()) {
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "IVFParentRows", ObCtxIds::VECTOR_CTX_ID));
    const int64_t bytes = ObIvfCentroidPayload::get_size(vector_dim_);
    char *buf = static_cast<char *>(allocator.alloc(bytes));
    float *rotated = input_vectors_are_rotated_ ? nullptr
        : static_cast<float *>(allocator.alloc(vector_dim_ * sizeof(float)));
    if (buf == nullptr || (!input_vectors_are_rotated_ && rotated == nullptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate DDL parent centroid buffers", K(ret),
          K_(vector_dim), K(center_count), "payload_bytes", bytes,
          "vector_bytes", input_vectors_are_rotated_ ? 0 : vector_dim_ * sizeof(float), KP(buf), KP(rotated));
    }
    for (int64_t p = 0; OB_SUCC(ret) && p < centroid_layout_->parent_count(); ++p) {
      const float *vector = centroid_layout_->parent_vector(p);
      if (!input_vectors_are_rotated_) {
        MEMCPY(rotated, vector, vector_dim_ * sizeof(float));
        ret = rotator_.rotate(vector_dim_, rotated);
        vector = rotated;
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to rotate IVF parent centroid", K(ret), K(p));
      } else if (OB_FAIL(ObIvfCentroidPayload::encode(buf, bytes, vector,
          vector_dim_, centroid_layout_->group_count(p)))) {
        LOG_WARN("[DISK_BBQ] failed to encode DDL parent centroid", K(ret), K(p), K_(vector_dim));
      } else if (OB_FAIL(append_sort_row_(IVF_ROW_TYPE_CENTROID, 0, p + 1,
          ObString(static_cast<int32_t>(bytes), buf)))) {
        LOG_WARN("[DISK_BBQ] failed to retain DDL parent centroid", K(ret), K(p));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!is_inited_ || OB_ISNULL(raw_writer_provider_)
      || quantization_finished_ || raw_write_finished_
      || vector_count <= 0 || center_count <= 0
      || posting_count < vector_count
      || posting_count > INT64_MAX - physical_centers
      || vector_count > INT64_MAX - physical_centers - posting_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF DDL quantization completion state", K(ret),
        K(vector_count), K(center_count), K(posting_count), K_(is_inited),
        K_(quantization_finished), K_(raw_write_finished), KP_(ddl_writer));
  } else if (FALSE_IT(expected_row_count = physical_centers + posting_count)) {
  } else if (centroid_row_count_ != physical_centers
      || one_bit_row_count_ != posting_count
      || output_row_count_ != expected_row_count
      || sorter_.get_row_count() != expected_row_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF DDL one-bit row count mismatch", K(ret),
        K(expected_row_count), K_(output_row_count),
        K_(centroid_row_count), K_(one_bit_row_count),
        K(vector_count), K(center_count), K(posting_count),
        "sorter_row_count", sorter_.get_row_count());
  }
  if (OB_SUCC(ret)) {
    ObITabletSliceWriter *writer = nullptr;
    ObIVFDDLWriterGuard writer_guard(nullptr == ddl_writer_ ? raw_writer_provider_ : nullptr, writer);
    // Legacy per-cluster callers do not install a layout; finish supplies K
    // before validating the one-based FLAT keys while draining the sorter.
    ivf_meta_.centroid_count_ = center_count;
    if (OB_FAIL(sorter_.finish())) {
      LOG_WARN("failed to finish IVF DDL posting sorter", K(ret));
    } else if (OB_FAIL(create_center_writer_(writer))) {
      LOG_WARN("failed to create IVF DDL centroid/posting writer", K(ret));
    } else if (OB_FAIL(write_sorted_rows_to_ddl_(*writer))) {
      LOG_WARN("failed to write IVF DDL centroid/posting slice", K(ret));
    }
    // Drain the initialized writer even on an append failure. The original
    // error wins and the final publication barrier remains closed.
    if (nullptr != writer) {
      const int close_ret = writer->close();
      if (OB_SUCCESS != close_ret) {
        LOG_WARN("failed to close IVF DDL centroid/posting slice", K(close_ret));
        if (OB_SUCC(ret)) {
          ret = close_ret;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("IVF DDL loaded quantization costs", K_(quant_encode_us), K_(sorter_wait_us), K_(sorter_append_us));
    ivf_meta_.centroid_count_ = center_count;
    ivf_meta_.vector_count_ = vector_count;
    quantization_finished_ = true;
    sorter_.reset();
    sorter_inited_ = false;
    centroid_layout_ = nullptr;
    leaf_posting_counts_.reset();
  }
  return ret;
}

int ObIVFDDLSSTableWriter::write_raw_vectors(
    ObIVFVectorReader &reader,
    const ObIArray<ObIVFVectorVID> &all_vids,
    const int64_t vid_begin,
    const int64_t vid_end,
    const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator task_allocator(
      ObMemAttr(MTL_ID(), "IVFRawWrite", ObCtxIds::VECTOR_CTX_ID));
  // Both the writer and reader reuse this buffer. Retain its capacity because
  // the task arena only frees old allocations when the entire range finishes.
  ObSEArray<float, 16, ObWrapperAllocator> vectors(
      OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(task_allocator));
  ObDatumRow row;
  ObITabletSliceWriter *writer = nullptr;
  ObIVFDDLWriterGuard writer_guard(raw_writer_provider_, writer);
  int64_t written_row_count = 0;
  const int64_t physical_column_count = row_desc_.payload_col_idx_ + 1;
  if (!is_inited_ || OB_ISNULL(raw_writer_provider_)
      || !quantization_finished_ || raw_write_finished_
      || all_vids.empty() || reader.get_dimension() != vector_dim_
      || vid_begin < 0 || vid_end <= vid_begin || vid_end > all_vids.count()
      || task_idx < 0 || task_idx >= ObIVFMergeProcessor::MAX_RAW_TASK_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF DDL RAW write range", K(ret), K(vid_begin), K(vid_end), K(task_idx));
  }
  for (int64_t i = MAX(1L, vid_begin); OB_SUCC(ret) && i < vid_end; ++i) {
    if (all_vids.at(i - 1) >= all_vids.at(i)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("IVF DDL RAW input is not strictly VID ordered", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    // Claim a task slot once. Payload processing and I/O never hold this lock.
    lib::ObMutexGuard guard(sorter_lock_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("failed to claim IVF DDL RAW range", K(ret), K(task_idx));
    } else if (raw_ranges_[task_idx].begin_ >= 0) {
      ret = OB_INIT_TWICE;
      LOG_WARN("IVF DDL RAW task already started", K(ret), K(task_idx));
    } else {
      raw_ranges_[task_idx].begin_ = vid_begin;
      raw_ranges_[task_idx].end_ = vid_end;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row.init(task_allocator, physical_column_count))) {
    LOG_WARN("failed to initialize IVF DDL RAW row", K(ret));
  } else if (OB_FAIL(raw_writer_provider_->create_raw_writer(task_idx, writer))) {
    LOG_WARN("failed to create IVF DDL RAW writer", K(ret), K(task_idx));
  } else if (OB_ISNULL(writer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF DDL RAW writer is null", K(ret), K(task_idx));
  } else {
    row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    row.mvcc_row_flag_.set_last_multi_version_row(true);
    row.storage_datums_[row_desc_.row_type_col_idx_].set_int(IVF_ROW_TYPE_RAW_VECTOR);
    row.storage_datums_[row_desc_.centroid_id_col_idx_].set_int(IVF_RAW_VECTOR_CID);
    row.storage_datums_[row_desc_.trans_version_col_idx_].set_int(-snapshot_version_);
    row.storage_datums_[row_desc_.sql_sequence_col_idx_].set_int(0);
    row.count_ = static_cast<int16_t>(physical_column_count);
  }
  for (int64_t batch_begin = vid_begin;
       OB_SUCC(ret) && batch_begin < vid_end;
       batch_begin += IVF_DDL_QUANTIZE_BATCH_SIZE) {
    const int64_t batch_end = MIN(vid_end, batch_begin + IVF_DDL_QUANTIZE_BATCH_SIZE);
    vectors.reuse();
    if (OB_FAIL(reader.batch_get_by_vid(all_vids, batch_begin, batch_end, vectors))) {
      LOG_WARN("failed to read IVF DDL RAW batch", K(ret), K(batch_begin), K(batch_end));
    } else if (vectors.count() != (batch_end - batch_begin) * vector_dim_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected IVF DDL RAW batch size", K(ret), K(batch_begin), K(batch_end));
    }
    for (int64_t i = batch_begin; OB_SUCC(ret) && i < batch_end; ++i) {
      float *vector = &vectors.at((i - batch_begin) * vector_dim_);
      if (!input_vectors_are_rotated_ && OB_FAIL(rotator_.rotate(vector_dim_, vector))) {
        LOG_WARN("failed to rotate IVF DDL RAW vector", K(ret), K(i));
      } else {
        row.storage_datums_[row_desc_.vector_id_col_idx_].set_uint(all_vids.at(i));
        row.storage_datums_[row_desc_.payload_col_idx_].set_string(ObString(
            static_cast<int32_t>(vector_dim_ * sizeof(float)),
            reinterpret_cast<const char *>(vector)));
        if (OB_FAIL(writer->append_row(row))) {
          LOG_WARN("failed to stream IVF DDL RAW vector", K(ret), K(i), K(task_idx));
        } else {
          ++written_row_count;
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(share::dag_yield())) {
      LOG_WARN("IVF DDL RAW write interrupted", K(ret), K(task_idx));
    }
  }
  if (OB_NOT_NULL(writer)) {
    if (OB_SUCC(ret) && (written_row_count != vid_end - vid_begin
        || writer->get_row_count() != written_row_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("IVF DDL RAW row count mismatch", K(ret), K(task_idx), K(written_row_count));
    }
    const int close_ret = writer->close();
    if (OB_SUCCESS != close_ret) {
      LOG_WARN("failed to close IVF DDL RAW writer", K(close_ret), K(task_idx));
      if (OB_SUCC(ret)) {
        ret = close_ret;
      }
    }
  }
  if (OB_SUCC(ret)) {
    ATOMIC_AAF(&raw_vector_row_count_, written_row_count);
    ATOMIC_AAF(&output_row_count_, written_row_count);
    raw_ranges_[task_idx].finished_ = true;
  }
  return ret;
}

int ObIVFDDLSSTableWriter::write_sorted_rows_to_ddl_(ObITabletSliceWriter &writer)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator row_allocator(
      ObMemAttr(MTL_ID(), "IVFWriteRows", ObCtxIds::VECTOR_CTX_ID));
  ObDatumRow row;
  const ObIVFSortItem *item = nullptr;
  const int64_t physical_column_count = row_desc_.payload_col_idx_ + 1;
  int64_t centroid_row_count = 0;
  int64_t one_bit_row_count = 0;
  int64_t raw_vector_row_count = 0;
  int64_t written_row_count = 0;
  int64_t self_cid = 0, group_id = 0;
  if (!is_inited_
      || quantization_finished_ || raw_write_finished_
      || !row_desc_.is_valid()
      || !row_desc_.is_valid_for_row(physical_column_count)) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid IVF DDL output writer state", K(ret),
        K_(is_inited), K_(quantization_finished), K_(raw_write_finished),
        K_(row_desc), KP_(ddl_writer));
  } else if (OB_FAIL(row.init(row_allocator, physical_column_count))) {
    LOG_WARN("failed to initialize sorted IVF DDL output row", K(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(sorter_.get_next_item(item))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      }
      LOG_WARN("failed to get sorted IVF DDL output row", K(ret));
    } else if (OB_ISNULL(item)
        || (IVF_ROW_TYPE_CENTROID != item->row_type_
            && IVF_ROW_TYPE_LOW_PRECISION_VECTOR != item->row_type_)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("sorter returned invalid IVF DDL row", K(ret), KPC(item));
    } else if (IVF_ROW_TYPE_CENTROID == item->row_type_
        && (OB_SUCCESS != ivf_meta_.decode_centroid_key(item->cid_, item->vid_, self_cid, group_id)
            || item->payload_.length() != ObIvfCentroidPayload::get_size(vector_dim_))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF DDL centroid row", K(ret), KPC(item));
    } else if (IVF_ROW_TYPE_LOW_PRECISION_VECTOR == item->row_type_
        && (item->cid_ < ivf_meta_.leaf_cid_base()
            || item->cid_ - ivf_meta_.leaf_cid_base() >= ivf_meta_.centroid_count_
            || item->payload_.length() != one_bit_code_size_)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid one-bit IVF DDL posting row", K(ret), KPC(item));
    } else {
      row.row_flag_.reset();
      row.mvcc_row_flag_.reset();
      row.storage_datums_[row_desc_.row_type_col_idx_].set_int(item->row_type_);
      row.storage_datums_[row_desc_.centroid_id_col_idx_].set_int(item->cid_);
      row.storage_datums_[row_desc_.vector_id_col_idx_].set_uint(item->vid_);
      row.storage_datums_[row_desc_.trans_version_col_idx_].set_int(
          -snapshot_version_);
      row.storage_datums_[row_desc_.sql_sequence_col_idx_].set_int(0);
      row.storage_datums_[row_desc_.payload_col_idx_].set_string(item->payload_);
      row.count_ = static_cast<int16_t>(physical_column_count);
      row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      row.mvcc_row_flag_.set_last_multi_version_row(true);
      if (OB_FAIL(writer.append_row(row))) {
        LOG_WARN("failed to append sorted IVF DDL output row",
            K(ret), KPC(item));
      } else {
        ++written_row_count;
        if (IVF_ROW_TYPE_CENTROID == item->row_type_) {
          ++centroid_row_count;
        } else if (IVF_ROW_TYPE_LOW_PRECISION_VECTOR == item->row_type_) {
          ++one_bit_row_count;
        } else {
          ++raw_vector_row_count;
        }
      }
    }
  }
  if (OB_SUCC(ret)
      && (written_row_count != output_row_count_
          || centroid_row_count != centroid_row_count_
          || one_bit_row_count != one_bit_row_count_
          || raw_vector_row_count != raw_vector_row_count_
          || writer.get_row_count() != written_row_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF DDL writer row count mismatch", K(ret),
        K(written_row_count), K(centroid_row_count), K(one_bit_row_count),
        K(raw_vector_row_count), K_(output_row_count),
        K_(centroid_row_count), K_(one_bit_row_count),
        K_(raw_vector_row_count),
        "writer_row_count", writer.get_row_count());
  }
  return ret;
}

int ObIVFDDLSSTableWriter::finish_raw_write(const int64_t vector_count)
{
  int ret = OB_SUCCESS;
  int64_t next_vid_idx = 0;
  if (!is_inited_ || !quantization_finished_ || raw_write_finished_
      || vector_count <= 0
      || vector_count > INT64_MAX - centroid_row_count_ - one_bit_row_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF DDL RAW completion state", K(ret), K(vector_count));
  }
  // Slice number determines the order of meta-store concatenation. Validate
  // complete, non-overlapping VID coverage in that same order, not completion order.
  for (int64_t i = 0; OB_SUCC(ret) && i < ObIVFMergeProcessor::MAX_RAW_TASK_COUNT; ++i) {
    const RawRange &range = raw_ranges_[i];
    if (range.begin_ >= 0) {
      if (!range.finished_ || range.begin_ != next_vid_idx || range.end_ <= range.begin_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("incomplete or unordered IVF DDL RAW output", K(ret), K(i),
            K(range.begin_), K(range.end_), K(range.finished_), K(next_vid_idx));
      } else {
        next_vid_idx = range.end_;
      }
    }
  }
  if (OB_SUCC(ret) && (next_vid_idx != vector_count
      || ATOMIC_LOAD(&raw_vector_row_count_) != vector_count
      || ATOMIC_LOAD(&output_row_count_) != centroid_row_count_ + one_bit_row_count_ + vector_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF DDL output coverage or row count mismatch", K(ret), K(vector_count), K(next_vid_idx));
  }
  if (OB_SUCC(ret)) {
    raw_write_finished_ = true;
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase

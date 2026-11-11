/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_merge_sstable_writer.h"

#include "lib/utility/ob_sort.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/vector_index/quantization/ob_quantization_encoder.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/vector_index/ob_ivf_ddl_sstable_writer.h"
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
static const int64_t IVF_QUANTIZE_BATCH_SIZE = 4096;
static const int64_t IVF_OUTPUT_WRITER_IDX = 0;
}

// Adapt only the physical sink. DDL owns the common loaded encoding, compact
// posting sharding, blocking sorter locks and output completion checks. Merge
// retains its snapshot, index builder, object cleaner and macro sequence space.
class ObIVFMergeSSTableWriter::LoadedOutput final : public ObIVFDDLRawWriterProvider
{
public:
  class MacroSlice final : public ObITabletSliceWriter
  {
  public:
    MacroSlice(ObIVFMergeSSTableWriter &owner, int64_t idx)
      : owner_(owner), idx_(idx), closed_row_count_(0), opened_(false), closed_(false) {}
    ~MacroSlice() override { reset(); }
    int init(const ObWriteMacroParam &param) override { UNUSED(param); return OB_NOT_SUPPORTED; }
    void reset() override
    {
      // DDL output tasks own close(), including their failure paths. Cleanup
      // must not flush abandoned rows or add an index root during rollback.
      writer_.reset();
      closed_row_count_ = 0;
      opened_ = false;
      closed_ = true;
    }
    int append_row(const ObDatumRow &row) override
    {
      int ret = OB_SUCCESS;
      if (closed_) {
        ret = OB_STATE_NOT_MATCH;
      } else if (!opened_ && OB_FAIL(owner_.open_writer_(writer_, idx_))) {
        LOG_WARN("failed to open loaded merge macro writer", K(ret), K_(idx));
      } else {
        opened_ = true;
        if (OB_FAIL(writer_.append_row(row))) {
          LOG_WARN("failed to append loaded merge macro row", K(ret), K_(idx));
        }
      }
      return ret;
    }
    int append_batch(const ObBatchDatumRows &rows) override { UNUSED(rows); return OB_NOT_SUPPORTED; }
    int64_t get_row_count() const override
    { return closed_ ? closed_row_count_ : writer_.get_appended_row_count(); }
    int close() override
    {
      int ret = OB_SUCCESS;
      if (closed_) {
        ret = OB_INIT_TWICE;
      } else {
        ret = opened_ ? writer_.close() : OB_SUCCESS;
        closed_row_count_ = writer_.get_appended_row_count();
        closed_ = true;
        if (OB_SUCC(ret)) {
          // close() has drained IO and transferred the index root. Keep the
          // row count for validation without retaining this slice's buffers.
          writer_.reset();
          opened_ = false;
        }
      }
      return ret;
    }
    TO_STRING_KV(K_(idx), K_(opened), K_(closed));
  private:
    ObIVFMergeSSTableWriter &owner_;
    int64_t idx_;
    int64_t closed_row_count_;
    bool opened_, closed_;
    ObIvfMacroBlockWriter writer_;
  };

  explicit LoadedOutput(ObIVFMergeSSTableWriter &owner)
    : owner_(owner), centers_(owner, IVF_OUTPUT_WRITER_IDX), postings_(), writer_(owner.allocator_),
      center_count_(0), error_(OB_SUCCESS) {}
  ~LoadedOutput() override
  {
    for (auto *&posting : postings_) {
      if (posting != nullptr) {
        posting->~MacroSlice(); owner_.allocator_.free(posting); posting = nullptr;
      }
    }
  }
  int init(int64_t center_count, int64_t parallelism)
  {
    static_assert(MAX_QUANT_TASK_COUNT >= ObIVFMergeProcessor::MAX_POSTING_TASK_COUNT,
        "loaded posting writers must fit the reserved merge macro windows");
    int ret = writer_.init(centers_, *this, owner_.snapshot_version_, owner_.vector_dim_,
        static_cast<ObVectorIndexDistAlgorithm>(owner_.ivf_meta_.dist_type_),
        true /* merge inputs are already in the current FHT space */);
    if (OB_SUCC(ret)) { ret = writer_.prepare_loaded_output(center_count, parallelism); }
    if (OB_SUCC(ret) && owner_.centroid_layout_ != nullptr) {
      ret = writer_.set_centroid_layout(*owner_.centroid_layout_);
    }
    // Allocate all sinks before parallel tasks start; the owner's arena need
    // not support concurrent allocation. Writers open lazily on their task.
    for (int64_t i = 0; OB_SUCC(ret) && i < writer_.posting_write_task_count(); ++i) {
      if (nullptr == (postings_[i] = OB_NEWx(MacroSlice, &owner_.allocator_, owner_, i + 1))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
    if (OB_SUCC(ret)) { center_count_ = center_count; }
    return ret;
  }
  int create_posting_writer(int64_t task_idx, ObITabletSliceWriter *&writer) override
  {
    writer = nullptr;
    if (task_idx < 0 || task_idx >= writer_.posting_write_task_count() || postings_[task_idx] == nullptr) {
      return OB_INVALID_ARGUMENT;
    }
    writer = postings_[task_idx];
    return OB_SUCCESS;
  }
  int create_raw_writer(int64_t task_idx, ObITabletSliceWriter *&writer) override
  {
    UNUSED(task_idx); writer = nullptr; return OB_NOT_SUPPORTED;
  }
  ObIVFMergeSSTableWriter &owner_;
  MacroSlice centers_;
  MacroSlice *postings_[MAX_QUANT_TASK_COUNT];
  ObIVFDDLSSTableWriter writer_;
  int64_t center_count_;
  int error_;
};

ObIVFMergeSSTableWriter::~ObIVFMergeSSTableWriter()
{
  if (loaded_output_ != nullptr) {
    loaded_output_->~LoadedOutput();
    allocator_.free(loaded_output_);
    loaded_output_ = nullptr;
  }
}

int ObIVFMergeSSTableWriter::prepare_loaded_output(int64_t center_count, int64_t parallelism)
{
  int ret = OB_SUCCESS;
  if (!is_inited_ || quantization_finished_ || raw_write_finished_ || output_row_count_ != 0
      || quant_centers_ != nullptr || loaded_output_ != nullptr || center_count <= 0 || parallelism <= 0) {
    ret = OB_STATE_NOT_MATCH;
  } else if (nullptr == (loaded_output_ = OB_NEWx(LoadedOutput, &allocator_, *this))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(loaded_output_->init(center_count, parallelism))) {
    LOG_WARN("failed to initialize loaded IVF merge output", K(ret), K(center_count), K(parallelism));
    // Retain the failed preparation state; no other mode may claim its slots.
    loaded_output_->error_ = ret;
  } else {
    sorter_.reset();
    sorter_inited_ = false;
  }
  return ret;
}

int64_t ObIVFMergeSSTableWriter::posting_write_task_count() const
{
  return loaded_output_ == nullptr ? 0 : loaded_output_->writer_.posting_write_task_count();
}

int ObIVFMergeSSTableWriter::write_posting_range(int64_t task_idx)
{
  return loaded_output_ == nullptr ? OB_STATE_NOT_MATCH
      : ATOMIC_LOAD(&loaded_output_->error_) != OB_SUCCESS ? ATOMIC_LOAD(&loaded_output_->error_)
      : loaded_output_->writer_.write_posting_range(task_idx);
}

int ObIVFMergeSSTableWriter::write_loaded_centers(const ObIArray<float> &centers,
    int64_t dimension, int64_t begin, int64_t end)
{
  return loaded_output_ == nullptr ? OB_STATE_NOT_MATCH
      : ATOMIC_LOAD(&loaded_output_->error_) != OB_SUCCESS ? ATOMIC_LOAD(&loaded_output_->error_)
      : loaded_output_->writer_.write_loaded_centers(centers, dimension, begin, end);
}

int ObIVFMergeSSTableWriter::set_leaf_posting_counts(const ObIArray<uint32_t> &counts)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] merge writer is not initialized for posting counts", K(ret));
  } else if (loaded_output_ == nullptr || quantization_finished_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] loaded merge output is not ready for posting counts", K(ret),
        KP_(loaded_output), K_(quantization_finished));
  } else if (OB_FAIL(loaded_output_->writer_.set_leaf_posting_counts(counts))) {
    LOG_WARN("[DISK_BBQ] failed to install loaded merge posting counts", K(ret), "count", counts.count());
  }
  return ret;
}

int ObIVFMergeSSTableWriter::quantize_loaded_vectors(const ObIArray<float> &centers, int64_t dimension,
    const ObIArray<ObIVFVectorVID> &vids, const ObIArray<float> &vectors,
    const ObIArray<int64_t> &primary, const ObIArray<int64_t> *secondary)
{
  return loaded_output_ == nullptr ? OB_STATE_NOT_MATCH
      : ATOMIC_LOAD(&loaded_output_->error_) != OB_SUCCESS ? ATOMIC_LOAD(&loaded_output_->error_)
      : loaded_output_->writer_.quantize_loaded_vectors(centers, dimension, vids, vectors, primary, secondary);
}

int ObIVFMergeSSTableWriter::copy_loaded_postings(const ObIVFEncodedPostingSource &source,
    int64_t source_cid, int64_t target_cid, const ObIArray<ObIVFVectorVID> &expected_vids)
{
  int ret = OB_SUCCESS;
  if (loaded_output_ == nullptr || quantization_finished_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid IVF posting reuse state", K(ret), KP_(loaded_output), K_(quantization_finished));
  } else if (OB_FAIL(ATOMIC_LOAD(&loaded_output_->error_))) {
    LOG_WARN("IVF posting reuse output already failed", K(ret));
  }
  class Consumer final : public ObIVFEncodedPostingConsumer
  {
  public:
    Consumer(ObIVFDDLSSTableWriter &writer, int64_t cid, int64_t size,
        const ObIArray<ObIVFVectorVID> &expected, char *codes)
      : writer_(writer), cid_(cid), size_(size), expected_(expected), codes_(codes), count_(0)
    {
      rows_.set_attr(ObMemAttr(MTL_ID(), "IVFReuseCodes", ObCtxIds::VECTOR_CTX_ID));
    }
    int append(ObIVFVectorVID vid, const ObString &payload) override
    {
      int ret = OB_SUCCESS;
      if (count_ >= expected_.count() || vid != expected_.at(count_)
          || (count_ > 0 && vid <= expected_.at(count_ - 1)) || payload.length() != size_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid reused IVF posting", K(ret), K(vid), K_(cid), K_(count), K_(size),
            "expected_count", expected_.count(), "payload_size", payload.length());
      } else {
        char *code = codes_ + rows_.count() * size_;
        MEMCPY(code, payload.ptr(), size_);
        ObIVFBuildRow row;
        row.row_type_ = IVF_ROW_TYPE_LOW_PRECISION_VECTOR;
        row.cid_ = cid_; row.vid_ = vid; row.payload_.assign_ptr(code, size_);
        if (OB_SUCC(ret = rows_.push_back(row))) {
          ++count_;
          if (rows_.count() == IVF_QUANTIZE_BATCH_SIZE) { ret = flush(); }
        } else {
          LOG_WARN("failed to retain reused IVF posting row", K(ret), K(vid), K_(cid));
        }
      }
      return ret;
    }
    int flush()
    {
      int ret = rows_.empty() ? OB_SUCCESS : writer_.append_loaded_postings(rows_);
      if (ret == OB_SUCCESS) {
        rows_.reuse();
        if (OB_FAIL(share::dag_yield())) {
          LOG_WARN("reused IVF posting flush interrupted", K(ret), K_(cid));
        }
      } else {
        LOG_WARN("failed to flush reused IVF posting rows", K(ret), K_(cid));
      }
      return ret;
    }
    ObIVFDDLSSTableWriter &writer_;
    int64_t cid_, size_;
    const ObIArray<ObIVFVectorVID> &expected_;
    char *codes_;
    int64_t count_;
    ObSEArray<ObIVFBuildRow, 16> rows_;
  };
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "IVFReuseCodes", ObCtxIds::VECTOR_CTX_ID));
  char *codes = nullptr;
  if (OB_SUCC(ret) && (source_cid < 0 || target_cid < 0 || target_cid >= loaded_output_->center_count_
      || one_bit_code_size_ <= 0
      || one_bit_code_size_ > INT64_MAX / IVF_QUANTIZE_BATCH_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF posting reuse arguments", K(ret), K(source_cid), K(target_cid), K_(one_bit_code_size));
  } else if (OB_SUCC(ret) && nullptr == (codes = static_cast<char *>(
      allocator.alloc(IVF_QUANTIZE_BATCH_SIZE * one_bit_code_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF posting reuse codes", K(ret), K_(one_bit_code_size));
  }
  if (OB_SUCC(ret)) {
    Consumer consumer(loaded_output_->writer_, target_cid, one_bit_code_size_, expected_vids, codes);
    if (OB_FAIL(source.scan_postings(source_cid, consumer))) {
      LOG_WARN("failed to copy loaded IVF postings", K(ret), K(source_cid), K(target_cid));
    } else if (consumer.count_ != expected_vids.count()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("loaded IVF posting coverage mismatch", K(ret), K(source_cid), K(target_cid),
          K(consumer.count_), "expected", expected_vids.count());
    } else if (OB_FAIL(consumer.flush())) {
      LOG_WARN("failed to append loaded IVF reused postings", K(ret), K(source_cid), K(target_cid));
    } else {
      ATOMIC_AAF(&reused_posting_count_, consumer.count_);
    }
  }
  if (OB_FAIL(ret) && nullptr != loaded_output_ && !quantization_finished_) {
    (void)ATOMIC_BCAS(&loaded_output_->error_, OB_SUCCESS, ret);
  }
  return ret;
}

ObIVFMergeSSTableWriter::ObIVFMergeSSTableWriter(
    ObIAllocator &allocator)
  : centroid_layout_(nullptr),
    allocator_(allocator),
    data_store_desc_(nullptr),
    index_builder_(nullptr),
    data_macro_seq_window_(),
    snapshot_version_(0),
    vector_dim_(0),
    one_bit_code_size_(0),
    row_desc_(),
    ivf_meta_(),
    sorter_(),
    sorter_inited_(false),
    sorter_lock_(ObLatchIds::OB_IVF_BUILD_HELPER_LOCK),
    centroid_row_count_(0),
    one_bit_row_count_(0),
    raw_vector_row_count_(0),
    output_row_count_(0),
    reused_posting_count_(0),
    quant_centers_(nullptr),
    leaf_posting_counts_(),
    quant_range_count_(0),
    completed_quant_ranges_(0),
    quantized_center_count_(0),
    started_quant_ranges_(0),
    loaded_output_(nullptr),
    is_inited_(false),
    quantization_finished_(false),
    raw_write_finished_(false)
{
  leaf_posting_counts_.set_block_allocator(ObWrapperAllocator(allocator));
}

int ObIVFMergeSSTableWriter::set_centroid_layout(const ObIvfCentroidLayout &layout)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[DISK_BBQ] merge writer is not initialized for centroid layout", K(ret));
  } else if (!layout.is_ready() || layout.dimension() != vector_dim_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DISK_BBQ] invalid merge centroid layout", K(ret), K_(vector_dim),
        "layout_ready", layout.is_ready(), "layout_dimension", layout.dimension());
  } else if (centroid_layout_ != nullptr || quantization_finished_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[DISK_BBQ] invalid merge centroid layout installation state", K(ret),
        KP_(centroid_layout), K_(quantization_finished));
  } else if (loaded_output_ != nullptr && OB_FAIL(loaded_output_->writer_.set_centroid_layout(layout))) {
    LOG_WARN("[DISK_BBQ] failed to install loaded merge centroid layout", K(ret));
  } else {
    centroid_layout_ = &layout;
    layout.set_meta(ivf_meta_);
  }
  return ret;
}

int ObIVFMergeSSTableWriter::init(
    const ObDataStoreDesc &data_store_desc,
    ObSSTableIndexBuilder &index_builder,
    const ObMacroSeqWindow &data_macro_seq_window,
    const int64_t snapshot_version,
    const int64_t vector_dim,
    const ObVectorIndexDistAlgorithm dist_type)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator encoder_allocator(
      ObMemAttr(MTL_ID(), "IVFQuantInit", ObCtxIds::VECTOR_CTX_ID));
  quantization::ObQuantizationParams params = {};
  quantization::ObQuantizationEncoder encoder;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF merge SSTable writer init twice", K(ret));
  } else if (!data_store_desc.is_valid()
      || data_store_desc.sstable_index_builder_ != &index_builder
      || data_macro_seq_window.start_ < 0
      || data_macro_seq_window.end_ <= data_macro_seq_window.start_
      || data_macro_seq_window.end_ - data_macro_seq_window.start_
          < OUTPUT_WRITER_COUNT
      || snapshot_version <= 0 || vector_dim <= 0
      || dist_type < VIDA_L2 || dist_type >= VIDA_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF merge SSTable writer argument", K(ret),
        K(data_macro_seq_window), K(snapshot_version), K(vector_dim), K(dist_type),
        KP(data_store_desc.sstable_index_builder_), KP(&index_builder));
  } else if (vector_dim > INT32_MAX / static_cast<int64_t>(sizeof(float))) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("IVF vector dimension is too large", K(ret), K(vector_dim));
  } else if (OB_FAIL(row_desc_.init(IVF_SSTABLE_FORMAT_VERSION))) {
    LOG_WARN("failed to initialize IVF output row descriptor", K(ret));
  } else {
    params.dim_ = vector_dim;
    params.bits_ = 1;
    if (OB_FAIL(encoder.init(
        encoder_allocator,
        quantization::ObQuantizationType::RABITQ,
        dist_type,
        params))) {
      LOG_WARN("failed to initialize one-bit IVF encoder", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    data_store_desc_ = &data_store_desc;
    index_builder_ = &index_builder;
    data_macro_seq_window_ = data_macro_seq_window;
    index_builder_->set_is_async_index(true);
    snapshot_version_ = snapshot_version;
    vector_dim_ = vector_dim;
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
      LOG_WARN("invalid one-bit IVF output metadata",
          K(ret), K_(one_bit_code_size), K_(ivf_meta));
    } else {
      // Loaded and range output bypass the global sorter. Its workspace is
      // allocated only on the first global append.
      is_inited_ = true;
    }
  }
  return ret;
}

int ObIVFMergeSSTableWriter::ensure_sorter_inited_()
{
  int ret = OB_SUCCESS;
  if (!sorter_inited_) {
    if (OB_FAIL(sorter_.init(MTL_ID()))) {
      LOG_WARN("failed to initialize IVF physical-row sorter", K(ret));
    } else {
      sorter_inited_ = true;
    }
  }
  return ret;
}

int ObIVFMergeSSTableWriter::append_sort_row_(
    const int64_t row_type,
    const int64_t cid,
    const ObIVFVectorVID vid,
    const ObString &payload)
{
  int ret = OB_SUCCESS;
  ObIVFBuildRow row;
  if (!is_inited_ || raw_write_finished_
      || quantization_finished_ || quant_centers_ != nullptr || loaded_output_ != nullptr
      || cid < 0 || payload.empty()
      || (IVF_ROW_TYPE_CENTROID != row_type
      && IVF_ROW_TYPE_LOW_PRECISION_VECTOR != row_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid sorted IVF output row", K(ret), K(row_type), K(cid),
        K(vid), "payload_size", payload.length(), K_(is_inited),
        K_(quantization_finished), K_(raw_write_finished));
  } else {
    row.row_type_ = row_type;
    row.cid_ = cid;
    row.vid_ = vid;
    row.payload_ = payload;
    ObSpinLockGuard guard(sorter_lock_);
    if (output_row_count_ == INT64_MAX
        || (IVF_ROW_TYPE_CENTROID == row_type
            && centroid_row_count_ == INT64_MAX)
        || (IVF_ROW_TYPE_LOW_PRECISION_VECTOR == row_type
            && one_bit_row_count_ == INT64_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("IVF sorted output row count overflow", K(ret), K(row_type),
          K(cid), K(vid), K_(output_row_count), K_(centroid_row_count),
          K_(one_bit_row_count));
    } else if (OB_FAIL(ensure_sorter_inited_())) {
      LOG_WARN("failed to prepare IVF physical-row sorter", K(ret));
    } else if (OB_FAIL(sorter_.append_row(row))) {
      LOG_WARN("failed to append IVF physical row to sorter", K(ret), K(row));
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

class ObIVFMergeSSTableWriter::QuantizationRange final : public ObIVFMergeQuantizer
{
public:
  bool supports_encoded_posting_reuse() const override { return true; }
  QuantizationRange(ObIVFMergeSSTableWriter &owner, ObIAllocator &allocator,
      int64_t begin, int64_t end)
    : owner_(owner), allocator_(allocator), begin_(begin), end_(end), next_cid_(begin),
      rows_(0), previous_cid_(-1), previous_vid_(0), opened_(false),
      row_allocator_(ObMemAttr(MTL_ID(), "IVFQuantRows", ObCtxIds::VECTOR_CTX_ID)) {}
  int open(int64_t task_idx)
  {
    int ret = owner_.open_writer_(writer_, task_idx + 1);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to open IVF posting range writer", K(ret), K(task_idx), K(begin_), K(end_));
    }
    if (OB_SUCC(ret)) {
      opened_ = true;
      ret = row_.init(row_allocator_, owner_.row_desc_.payload_col_idx_ + 1);
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to initialize IVF posting range row", K(ret), K(task_idx));
      }
    }
    return ret;
  }
  int append(int64_t cid, ObIVFVectorVID vid, const ObString &payload)
  {
    int ret = OB_SUCCESS;
    if (!opened_ || cid != next_cid_ || cid < begin_ || cid >= end_
        || payload.length() != owner_.one_bit_code_size_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid IVF quantization posting", K(ret), K(cid), K(vid), K(next_cid_), K(begin_), K(end_),
          K(payload.length()));
    } else if (cid == previous_cid_ && vid <= previous_vid_) {
      ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
      LOG_WARN("IVF quantization postings are not strictly ordered", K(ret), K(cid), K(vid), K(previous_cid_),
          K(previous_vid_));
    } else {
      ObIVFSortItem item;
      item.row_type_ = IVF_ROW_TYPE_LOW_PRECISION_VECTOR;
      item.cid_ = owner_.ivf_meta_.leaf_storage_cid(cid); item.vid_ = vid; item.payload_ = payload;
      if (OB_SUCC(ret = owner_.append_macro_row_(writer_, item, row_))) {
        ++rows_; previous_cid_ = cid; previous_vid_ = vid;
      } else {
        LOG_WARN("failed to append IVF quantization macro row", K(ret), K(cid), K(vid));
      }
    }
    return ret;
  }
  int quantize_cluster(ObIVFVectorReader &reader, const ObIArray<float> &centers,
      int64_t k, int64_t cid, const ObIArray<ObIVFVectorVID> &members,
      const ObIArray<ObIVFVectorVID> *secondary) override
  {
    int ret = validate(centers, k, cid);
    if (OB_SUCC(ret)) ret = owner_.quantize_cluster_(reader, centers, k, cid,
        members, secondary, nullptr, -1, 0, this);
    if (OB_SUCC(ret)) ++next_cid_;
    return ret;
  }
  int quantize_cluster_with_reuse(ObIVFVectorReader &reader, const ObIArray<float> &centers,
      int64_t k, int64_t cid, const ObIArray<ObIVFVectorVID> &members,
      const ObIArray<ObIVFVectorVID> *secondary, const ObIVFEncodedPostingSource &source,
      int64_t source_cid, int64_t prefix) override
  {
    int ret = validate(centers, k, cid);
    if (OB_SUCC(ret)) ret = owner_.quantize_cluster_(reader, centers, k, cid,
        members, secondary, &source, source_cid, prefix, this);
    if (OB_SUCC(ret)) ++next_cid_;
    return ret;
  }
  int validate(const ObIArray<float> &centers, int64_t k, int64_t cid) const
  {
    const int ret = !opened_ || &centers != owner_.quant_centers_ || cid != next_cid_
        || cid >= end_ || k != centers.count() / owner_.vector_dim_
        ? OB_STATE_NOT_MATCH : OB_SUCCESS;
    if (OB_SUCCESS != ret) {
      LOG_WARN("invalid IVF quantization range state", K(ret), K(opened_), K(cid), K(next_cid_), K(end_),
          K(k));
    }
    return ret;
  }
  int close(bool success)
  {
    int ret = OB_SUCCESS;
    if (success && next_cid_ != end_) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("IVF quantization range closed before completion", K(ret), K(next_cid_), K(end_), K(rows_));
    }
    if (opened_) {
      // close drains pending I/O, including on an aborted task. The DAG owns
      // rollback of all physical output until every range has succeeded.
      const int close_ret = writer_.close();
      if (OB_SUCCESS != close_ret) {
        LOG_WARN("failed to close IVF posting range writer", K(close_ret), K(ret), K(begin_), K(end_));
      }
      if (OB_SUCC(ret)) ret = close_ret;
      opened_ = false;
    }
    if (success && OB_SUCC(ret)) {
      ATOMIC_AAF(&owner_.one_bit_row_count_, rows_);
      ATOMIC_AAF(&owner_.output_row_count_, rows_);
      ATOMIC_AAF(&owner_.quantized_center_count_, end_ - begin_);
      ATOMIC_INC(&owner_.completed_quant_ranges_);
    }
    return ret;
  }
  int finish_quantization(int64_t n, int64_t k, int64_t p) override
  {
    int ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot finish IVF quantization through range writer", K(ret), K(n), K(k), K(p));
    return ret;
  }
  int write_raw_vectors(ObIVFVectorReader &r, const ObIArray<ObIVFVectorVID> &v,
      int64_t b, int64_t e, int64_t t) override
  {
    UNUSEDx(r, v);
    int ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot write IVF RAW vectors through quantization range writer", K(ret), K(b), K(e), K(t));
    return ret;
  }
  int finish_raw_write(int64_t n) override
  {
    int ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot finish IVF RAW write through quantization range writer", K(ret), K(n));
    return ret;
  }
  ObIVFMergeSSTableWriter &owner_;
  ObIAllocator &allocator_;
  int64_t begin_, end_, next_cid_, rows_, previous_cid_;
  ObIVFVectorVID previous_vid_;
  bool opened_;
  ObArenaAllocator row_allocator_;
  ObDatumRow row_;
  ObIvfMacroBlockWriter writer_;
};

int ObIVFMergeSSTableWriter::prepare_quantization(const ObIArray<float> &centers,
    int64_t center_count, int64_t &task_count)
{
  int ret = OB_SUCCESS;
  task_count = 0;
  if (!is_inited_ || quant_centers_ != nullptr || loaded_output_ != nullptr || output_row_count_ != 0
      || quantization_finished_ || center_count <= 0
      || center_count > INT64_MAX / vector_dim_
      || centers.count() != center_count * vector_dim_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("invalid IVF quantization preparation state", K(ret), K(is_inited_), K(center_count),
        K(centers.count()), K(vector_dim_), K(output_row_count_));
  } else if (OB_FAIL(leaf_posting_counts_.prepare_allocate(center_count))) {
    LOG_WARN("failed to allocate leaf posting counts", K(ret), K(center_count));
  } else {
    MEMSET(leaf_posting_counts_.get_data(), 0, center_count * sizeof(uint32_t));
    sorter_.reset();
    sorter_inited_ = false;
    quant_centers_ = &centers;
    ivf_meta_.centroid_count_ = center_count;
    task_count = quant_range_count_ = MIN(MAX_QUANT_TASK_COUNT, center_count);
  }
  return ret;
}

int ObIVFMergeSSTableWriter::open_quantization_range(int64_t begin, int64_t end,
    int64_t task_idx, ObIAllocator &allocator, ObIVFMergeQuantizer *&output)
{
  int ret = OB_SUCCESS;
  output = nullptr;
  const int64_t k = quant_centers_ == nullptr ? 0 : quant_centers_->count() / vector_dim_;
  if (quant_range_count_ <= 0 || task_idx < 0 || task_idx >= quant_range_count_
      || begin < 0 || end <= begin || end > k) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF quantization range", K(ret), K(begin), K(end), K(task_idx), K(k),
        K(quant_range_count_));
  } else {
    const int64_t width = k / quant_range_count_, extra = k % quant_range_count_;
    const int64_t expected_begin = task_idx * width + MIN(task_idx, extra);
    if (begin != expected_begin || end != begin + width + (task_idx < extra ? 1 : 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("IVF quantization range does not match task", K(ret), K(begin), K(end), K(task_idx),
          K(expected_begin), K(width), K(extra));
    } else {
      ObSpinLockGuard guard(sorter_lock_);
      const uint64_t bit = uint64_t(1) << task_idx;
      if ((started_quant_ranges_ & bit) != 0) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("IVF quantization range already started", K(ret), K(task_idx), K(started_quant_ranges_));
      } else {
        started_quant_ranges_ |= bit;
      }
    }
  }
  QuantizationRange *range = nullptr;
  if (OB_SUCC(ret) && OB_ISNULL(range = OB_NEWx(QuantizationRange, &allocator,
      *this, allocator, begin, end))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF quantization range", K(ret), K(begin), K(end), K(task_idx));
  } else if (OB_SUCC(ret)) {
    ret = range->open(task_idx);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to open IVF quantization range", K(ret), K(task_idx), K(begin), K(end));
    }
  }
  if (OB_SUCC(ret)) {
    output = range;
  } else if (range != nullptr) {
    (void)range->close(false);
    range->~QuantizationRange(); allocator.free(range);
  }
  return ret;
}

int ObIVFMergeSSTableWriter::close_quantization_range(ObIVFMergeQuantizer *&output, bool success)
{
  int ret = OB_SUCCESS;
  if (output == nullptr || output == this) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF quantization range to close", K(ret), K(success));
  } else {
    QuantizationRange *range = static_cast<QuantizationRange *>(output);
    ret = range->close(success);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to close IVF quantization range", K(ret), K(success));
    }
    ObIAllocator &allocator = range->allocator_;
    range->~QuantizationRange(); allocator.free(range); output = nullptr;
  }
  return ret;
}

int ObIVFMergeSSTableWriter::append_posting_(QuantizationRange *range,
    int64_t cid, ObIVFVectorVID vid, const ObString &payload)
{
  return range == nullptr ? append_sort_row_(IVF_ROW_TYPE_LOW_PRECISION_VECTOR, ivf_meta_.leaf_storage_cid(cid), vid, payload)
      : range->append(cid, vid, payload);
}

class ObIVFMergeSSTableWriter::ReusePostingConsumer final : public ObIVFEncodedPostingConsumer
{
public:
  ReusePostingConsumer(ObIVFMergeSSTableWriter &writer, const int64_t cid,
      const ObIArray<ObIVFVectorVID> &members, const int64_t count,
      QuantizationRange *range, const ObIArray<ObIVFVectorVID> &encoded_vids, const char *codes)
    : writer_(writer), cid_(cid), members_(members), expected_(count), count_(0),
      range_(range), encoded_vids_(encoded_vids), codes_(codes), encoded_pos_(0) {}
  virtual int append(const ObIVFVectorVID vid, const ObString &payload) override
  {
    int ret = OB_SUCCESS;
    if (count_ >= expected_ || members_.at(count_) != vid
        || (count_ > 0 && vid <= members_.at(count_ - 1))
        || payload.length() != writer_.one_bit_code_size_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid reusable IVF encoded posting", K(ret), K(cid_), K(vid), K(count_), K(expected_),
          K(payload.length()));
    } else {
      // Old and inserted VIDs may interleave. Merge the borrowed old stream
      // with the freshly encoded rows; never buffer all old encoded payloads.
      while (OB_SUCC(ret) && range_ != nullptr && encoded_pos_ < encoded_vids_.count()
          && encoded_vids_.at(encoded_pos_) < vid) {
        const ObString code(static_cast<int32_t>(writer_.one_bit_code_size_),
            codes_ + encoded_pos_ * writer_.one_bit_code_size_);
        ret = range_->append(cid_, encoded_vids_.at(encoded_pos_), code);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to interleave freshly encoded IVF posting", K(ret), K(cid_), K(encoded_pos_));
        }
        if (OB_SUCC(ret)) ++encoded_pos_;
      }
      if (OB_SUCC(ret) && range_ != nullptr && encoded_pos_ < encoded_vids_.count()
          && encoded_vids_.at(encoded_pos_) == vid) {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        LOG_WARN("duplicate VID in reused and encoded IVF postings", K(ret), K(cid_), K(vid),
            K(encoded_pos_));
      } else if (OB_SUCC(ret) && OB_SUCC(ret = writer_.append_posting_(range_, cid_, vid, payload))) {
        ++count_;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to append reusable IVF posting", K(ret), K(cid_), K(vid));
      }
    }
    return ret;
  }
  ObIVFMergeSSTableWriter &writer_;
  int64_t cid_;
  const ObIArray<ObIVFVectorVID> &members_;
  int64_t expected_;
  int64_t count_;
  QuantizationRange *range_;
  const ObIArray<ObIVFVectorVID> &encoded_vids_;
  const char *codes_;
  int64_t encoded_pos_;
};

int ObIVFMergeSSTableWriter::quantize_cluster(
    ObIVFVectorReader &reader,
    const ObIArray<float> &final_centers,
    const int64_t center_count,
    const int64_t cid,
    const ObIArray<ObIVFVectorVID> &primary_member_vids,
    const ObIArray<ObIVFVectorVID> *secondary_member_vids)
{
  return quantize_cluster_(reader, final_centers, center_count, cid,
      primary_member_vids, secondary_member_vids, nullptr, -1, 0);
}

int ObIVFMergeSSTableWriter::quantize_cluster_with_reuse(
    ObIVFVectorReader &reader, const ObIArray<float> &final_centers,
    const int64_t center_count, const int64_t cid,
    const ObIArray<ObIVFVectorVID> &member_vids,
    const ObIArray<ObIVFVectorVID> *secondary_member_vids,
    const ObIVFEncodedPostingSource &source, const int64_t source_center_idx,
    const int64_t reused_prefix_count)
{
  return quantize_cluster_(reader, final_centers, center_count, cid,
      member_vids, secondary_member_vids, &source, source_center_idx, reused_prefix_count);
}

int ObIVFMergeSSTableWriter::quantize_cluster_(
    ObIVFVectorReader &reader, const ObIArray<float> &final_centers,
    const int64_t center_count, const int64_t cid,
    const ObIArray<ObIVFVectorVID> &primary_member_vids,
    const ObIArray<ObIVFVectorVID> *secondary_member_vids,
    const ObIVFEncodedPostingSource *source, const int64_t source_center_idx,
    const int64_t reused_prefix_count, QuantizationRange *range)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator task_allocator(
      ObMemAttr(MTL_ID(), "IVFQuantTask", ObCtxIds::VECTOR_CTX_ID));
  quantization::ObQuantizationParams params = {};
  quantization::ObQuantizationEncoder encoder;
  ObArray<ObIVFVectorVID, ObWrapperAllocator> posting_vids;
  ObSEArray<float, 16, ObWrapperAllocator> vectors(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(task_allocator));
  char *one_bit_codes = nullptr;
  const float *center = nullptr;
  uint32_t posting_count = 0;
  uint32_t total = 0;
  posting_vids.set_block_allocator(ObWrapperAllocator(task_allocator));
  if ((range == nullptr && quant_centers_ != nullptr)
      || reused_prefix_count < 0 || reused_prefix_count > primary_member_vids.count()
      || (source == nullptr && (reused_prefix_count != 0 || source_center_idx != -1))
      || (source != nullptr && source_center_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF quantization reuse arguments", K(ret), K(cid), K(source_center_idx),
        K(reused_prefix_count), K(primary_member_vids.count()));
  } else if (OB_FAIL(ObIvfCentroidPayload::add_child_count(total, primary_member_vids.count()))) {
    LOG_WARN("IVF primary posting count overflow", K(ret), K(cid));
  } else if (OB_NOT_NULL(secondary_member_vids)
      && OB_FAIL(ObIvfCentroidPayload::add_child_count(total, secondary_member_vids->count()))) {
    LOG_WARN("IVF posting count overflow", K(ret), K(cid), K(total),
        "secondary_count", secondary_member_vids->count());
  } else {
    posting_count = total - static_cast<uint32_t>(reused_prefix_count);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
  } else if (!is_inited_ || quantization_finished_
      || reader.get_dimension() != vector_dim_
      || center_count <= 0 || cid < 0 || cid >= center_count
      || center_count > INT64_MAX / vector_dim_
      || final_centers.count() != center_count * vector_dim_
      || (primary_member_vids.empty() && source == nullptr)
      || (posting_count == 0 && source == nullptr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF cluster quantization argument", K(ret),
        K(center_count), K(cid), K_(vector_dim),
        "reader_dim", reader.get_dimension(),
        "center_value_count", final_centers.count(),
        "primary_count", primary_member_vids.count());
  } else {
    center = &final_centers.at(cid * vector_dim_);
    params.dim_ = vector_dim_;
    params.bits_ = 1;
    if (OB_FAIL(encoder.init(
        task_allocator,
        quantization::ObQuantizationType::RABITQ,
        static_cast<ObVectorIndexDistAlgorithm>(ivf_meta_.dist_type_),
        params))) {
      LOG_WARN("failed to initialize one-bit encoder for IVF cluster",
          K(ret), K(cid));
    } else if (encoder.code_size() != one_bit_code_size_
        || posting_count > INT64_MAX / one_bit_code_size_) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("IVF one-bit code size mismatch or overflow", K(ret), K(cid), K(posting_count),
          K(one_bit_code_size_), K(encoder.code_size()));
    } else if (posting_count > 0 && OB_ISNULL(one_bit_codes = static_cast<char *>(
        task_allocator.alloc_align(posting_count * one_bit_code_size_, alignof(float))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate one-bit IVF codes",
          K(ret), K(cid), K(posting_count), K_(one_bit_code_size));
    }
  }

  if (OB_SUCC(ret) && OB_FAIL(posting_vids.reserve(posting_count))) {
    LOG_WARN("failed to reserve IVF posting VIDs",
        K(ret), K(cid), K(posting_count));
  }
  for (int64_t i = reused_prefix_count; OB_SUCC(ret) && i < primary_member_vids.count(); ++i) {
    ret = posting_vids.push_back(primary_member_vids.at(i));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to append primary IVF posting VID", K(ret), K(cid),
          K(i), K(primary_member_vids.at(i)));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && OB_NOT_NULL(secondary_member_vids)
      && i < secondary_member_vids->count(); ++i) {
    ret = posting_vids.push_back(secondary_member_vids->at(i));
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to append secondary IVF posting VID", K(ret), K(cid),
          K(i), K(secondary_member_vids->at(i)));
    }
  }
  if (OB_SUCC(ret)) {
    lib::ob_sort(posting_vids.begin(), posting_vids.end());
    for (int64_t i = 1; OB_SUCC(ret) && i < posting_vids.count(); ++i) {
      if (posting_vids.at(i - 1) == posting_vids.at(i)) {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        LOG_WARN("duplicate one-bit posting in IVF cluster",
            K(ret), K(cid), K(posting_vids.at(i)));
      }
    }
  }

  for (int64_t batch_begin = 0;
       OB_SUCC(ret) && batch_begin < posting_vids.count();
       batch_begin += IVF_QUANTIZE_BATCH_SIZE) {
    const int64_t batch_end = MIN(
        posting_vids.count(), batch_begin + IVF_QUANTIZE_BATCH_SIZE);
    vectors.reuse();
    if (OB_FAIL(reader.batch_get_by_vid(
        posting_vids, batch_begin, batch_end, vectors))) {
      LOG_WARN("failed to read vectors for one-bit quantization",
          K(ret), K(cid), K(batch_begin), K(batch_end));
    } else if (vectors.count()
        != (batch_end - batch_begin) * vector_dim_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected IVF quantization vector count", K(ret), K(cid),
          K(batch_begin), K(batch_end), "value_count", vectors.count());
    }
    for (int64_t member_idx = batch_begin;
         OB_SUCC(ret) && member_idx < batch_end;
         ++member_idx) {
      const int64_t local_idx = member_idx - batch_begin;
      const float *vector = &vectors.at(local_idx * vector_dim_);
      if (OB_FAIL(encoder.encode(
          vector, center,
          {reinterpret_cast<uint8_t *>(one_bit_codes
              + member_idx * one_bit_code_size_),
           one_bit_code_size_}))) {
        LOG_WARN("failed to encode one-bit IVF posting",
            K(ret), K(cid), K(posting_vids.at(member_idx)));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(share::dag_yield())) {
      LOG_WARN("one-bit IVF quantization was interrupted",
          K(ret), K(cid), K(batch_begin), K(batch_end));
    }
  }

  int64_t encoded_begin = 0;
  if (OB_SUCC(ret) && source != nullptr) {
    ReusePostingConsumer consumer(*this, cid, primary_member_vids, reused_prefix_count,
        range, posting_vids, one_bit_codes);
    if (OB_FAIL(source->scan_postings(source_center_idx, consumer))) {
      LOG_WARN("failed to copy IVF encoded postings", K(ret), K(cid), K(source_center_idx));
    } else if (consumer.count_ != reused_prefix_count) {
      ret = OB_INVALID_DATA;
      LOG_WARN("IVF reused posting count mismatch", K(ret), K(cid), K(source_center_idx),
          K(reused_prefix_count), K(consumer.count_));
    } else {
      encoded_begin = consumer.encoded_pos_;
      (void)ATOMIC_AAF(&reused_posting_count_, consumer.count_);
    }
  }
  for (int64_t i = encoded_begin; OB_SUCC(ret) && i < posting_vids.count(); ++i) {
    const ObString code(static_cast<int32_t>(one_bit_code_size_), one_bit_codes + i * one_bit_code_size_);
    ret = append_posting_(range, cid, posting_vids.at(i), code);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to retain encoded IVF posting", K(ret), K(cid), K(i), K(posting_vids.at(i)));
    }
  }

  if (OB_SUCC(ret)) {
    if (range != nullptr) {
      leaf_posting_counts_.at(cid) = total;
    } else {
      const int64_t bytes = ObIvfCentroidPayload::get_size(vector_dim_);
      char *buf = static_cast<char *>(task_allocator.alloc(bytes));
      if (buf == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("[DISK_BBQ] failed to allocate merge centroid payload", K(ret), K(cid),
            K_(vector_dim), "alloc_size", bytes);
      } else if (OB_FAIL(ObIvfCentroidPayload::encode(buf, bytes, center, vector_dim_, total))) {
        LOG_WARN("[DISK_BBQ] failed to encode merge leaf centroid", K(ret), K(cid), K_(vector_dim), K(total));
      } else {
        const ObString payload(static_cast<int32_t>(bytes), buf);
        const int64_t self = ivf_meta_.leaf_storage_cid(cid);
        if (!ivf_meta_.is_hierarchical()) {
          if (OB_FAIL(append_sort_row_(IVF_ROW_TYPE_CENTROID, 0, self, payload))) {
            LOG_WARN("[DISK_BBQ] failed to retain merge flat centroid", K(ret), K(cid), K(self));
          }
        } else if (centroid_layout_ == nullptr) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("[DISK_BBQ] missing centroid layout during merge write", K(ret), K(cid), K(self));
        } else if (OB_FAIL(append_sort_row_(IVF_ROW_TYPE_CENTROID, centroid_layout_->primary_parent(cid), self, payload))) {
          LOG_WARN("[DISK_BBQ] failed to retain merge primary child centroid", K(ret), K(cid), K(self));
        } else if (centroid_layout_->secondary_parent(cid) > 0) {
          if (OB_FAIL(append_sort_row_(IVF_ROW_TYPE_CENTROID,
              centroid_layout_->secondary_parent(cid), self, payload))) {
            LOG_WARN("[DISK_BBQ] failed to retain merge secondary child centroid", K(ret), K(cid), K(self));
          }
        }
      }
    }
  }
  return ret;
}

int ObIVFMergeSSTableWriter::finish_quantization_ranges_(
    int64_t vector_count, int64_t center_count, int64_t posting_count)
{
  int ret = OB_SUCCESS;
  const int64_t physical_centers = centroid_layout_ == nullptr ? center_count : centroid_layout_->center_row_count();
  if (!is_inited_ || quantization_finished_ || raw_write_finished_ || quant_centers_ == nullptr
      || center_count <= 0 || vector_count <= 0 || posting_count < vector_count
      || posting_count > INT64_MAX - physical_centers
      || quant_centers_->count() / vector_dim_ != center_count
      || completed_quant_ranges_ != quant_range_count_ || quantized_center_count_ != center_count
      || one_bit_row_count_ != posting_count || output_row_count_ != posting_count
      || centroid_row_count_ != 0) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("IVF quantization completion state mismatch", K(ret), K(vector_count), K(center_count),
        K(posting_count), K(completed_quant_ranges_), K(quant_range_count_), K(quantized_center_count_),
        K(one_bit_row_count_), K(output_row_count_));
  } else {
    // Keep all centroid keys in their own writer. Mixing centers and postings
    // in each range would create overlapping macro-block key intervals.
    ObIvfMacroBlockWriter writer;
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "IVFCenterRows", ObCtxIds::VECTOR_CTX_ID));
    ObDatumRow row;
    bool opened = false;
    if (OB_FAIL(row.init(allocator, row_desc_.payload_col_idx_ + 1))) {
      LOG_WARN("failed to initialize IVF centroid output row", K(ret), K(center_count));
    } else if (OB_FAIL(open_writer_(writer, IVF_OUTPUT_WRITER_IDX))) {
      LOG_WARN("failed to open IVF centroid output writer", K(ret), K(center_count));
    } else {
      opened = true;
      const int64_t bytes = ObIvfCentroidPayload::get_size(vector_dim_);
      char *buf = static_cast<char *>(allocator.alloc(bytes));
      if (buf == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("[DISK_BBQ] failed to allocate merge centroid row buffer", K(ret),
            K(center_count), K_(vector_dim), "alloc_size", bytes);
      }
      auto emit_center = [&](int64_t key_cid, int64_t key_vid, const float *vec, uint32_t children) -> int {
        int rc = ObIvfCentroidPayload::encode(buf, bytes, vec, vector_dim_, children);
        ObIVFSortItem item;
        item.row_type_ = IVF_ROW_TYPE_CENTROID; item.cid_ = key_cid; item.vid_ = key_vid;
        item.payload_.assign_ptr(buf, static_cast<int32_t>(bytes));
        if (rc == OB_SUCCESS) { rc = append_macro_row_(writer, item, row); }
        if (rc == OB_SUCCESS) {
          ++centroid_row_count_; ++output_row_count_;
          if (centroid_row_count_ % 128 == 0) { rc = share::dag_yield(); }
        }
        return rc;
      };
      if (ivf_meta_.is_hierarchical()) {
        if (centroid_layout_ == nullptr) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("[DISK_BBQ] missing centroid layout when finishing merge ranges", K(ret), K(center_count));
        }
        for (int64_t p = 0; OB_SUCC(ret) && p < ivf_meta_.parent_centroid_count_; ++p) {
          ret = emit_center(0, p + 1, centroid_layout_->parent_vector(p), centroid_layout_->group_count(p));
        }
        for (int64_t p = 0; OB_SUCC(ret) && p < ivf_meta_.parent_centroid_count_; ++p) {
          for (int64_t pos = centroid_layout_->group_begin(p);
               OB_SUCC(ret) && pos < centroid_layout_->group_end(p); ++pos) {
            const int64_t j = centroid_layout_->member(pos);
            ret = emit_center(p + 1, ivf_meta_.leaf_storage_cid(j),
                &quant_centers_->at(j * vector_dim_), leaf_posting_counts_.at(j));
          }
        }
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < center_count; ++j) {
          ret = emit_center(0, j + 1, &quant_centers_->at(j * vector_dim_), leaf_posting_counts_.at(j));
        }
      }
    }
    if (opened) {
      const int close_ret = writer.close();
      if (OB_SUCCESS != close_ret) {
        LOG_WARN("failed to close IVF centroid writer", K(close_ret), K(ret), K(center_count));
      }
      if (OB_SUCC(ret)) ret = close_ret;
    }
  }
  if (OB_SUCC(ret)) {
    ivf_meta_.centroid_count_ = center_count;
    ivf_meta_.vector_count_ = vector_count;
    quantization_finished_ = true;
    quant_centers_ = nullptr;
    centroid_layout_ = nullptr;
    leaf_posting_counts_.reset();
    LOG_INFO("IVF quantization reuse summary", K(vector_count), K(center_count), K(posting_count),
        "reused_postings", ATOMIC_LOAD(&reused_posting_count_),
        "encoded_postings", posting_count - ATOMIC_LOAD(&reused_posting_count_),
        "posting_writers", quant_range_count_);
  }
  return ret;
}

int ObIVFMergeSSTableWriter::finish_quantization(
    const int64_t vector_count,
    const int64_t center_count,
    const int64_t posting_count)
{
  if (loaded_output_ != nullptr) {
    int ret = ATOMIC_LOAD(&loaded_output_->error_);
    if (OB_SUCC(ret)) {
      ret = loaded_output_->writer_.finish_quantization(vector_count, center_count, posting_count);
    }
    if (OB_SUCC(ret)) {
      centroid_row_count_ = centroid_layout_ == nullptr ? center_count : centroid_layout_->center_row_count();
      one_bit_row_count_ = posting_count;
      output_row_count_ = loaded_output_->writer_.get_row_count();
      ivf_meta_ = loaded_output_->writer_.get_ivf_meta();
      quantization_finished_ = true;
      centroid_layout_ = nullptr;
      LOG_INFO("IVF loaded quantization reuse summary", K(vector_count), K(center_count), K(posting_count),
          "reused_postings", ATOMIC_LOAD(&reused_posting_count_),
          "encoded_postings", posting_count - ATOMIC_LOAD(&reused_posting_count_),
          "posting_writers", posting_write_task_count());
      // All slices are closed and validated. RAW_WRITE uses only the copied
      // metadata/counters and the shared index builder, not these writers.
      loaded_output_->~LoadedOutput();
      allocator_.free(loaded_output_);
      loaded_output_ = nullptr;
    }
    return ret;
  }
  if (quant_range_count_ > 0) {
    return finish_quantization_ranges_(vector_count, center_count, posting_count);
  }
  int ret = OB_SUCCESS;
  int64_t expected_row_count = 0;
  const int64_t physical_centers = centroid_layout_ == nullptr ? center_count : centroid_layout_->center_row_count();
  if (is_inited_ && !quantization_finished_ && centroid_layout_ != nullptr && centroid_layout_->is_hierarchical()) {
    ObArenaAllocator alloc(ObMemAttr(MTL_ID(), "IVFParentRows", ObCtxIds::VECTOR_CTX_ID));
    const int64_t bytes = ObIvfCentroidPayload::get_size(vector_dim_);
    char *buf = static_cast<char *>(alloc.alloc(bytes));
    if (buf == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[DISK_BBQ] failed to allocate merge parent centroid payload", K(ret),
          K(center_count), K_(vector_dim), "alloc_size", bytes);
    }
    for (int64_t p = 0; OB_SUCC(ret) && p < centroid_layout_->parent_count(); ++p) {
      if (OB_FAIL(ObIvfCentroidPayload::encode(buf, bytes, centroid_layout_->parent_vector(p), vector_dim_,
          centroid_layout_->group_count(p)))) {
        LOG_WARN("[DISK_BBQ] failed to encode merge parent centroid", K(ret), K(p), K_(vector_dim));
      } else if (OB_FAIL(append_sort_row_(IVF_ROW_TYPE_CENTROID, 0, p + 1,
          ObString(static_cast<int32_t>(bytes), buf)))) {
        LOG_WARN("[DISK_BBQ] failed to retain merge parent centroid", K(ret), K(p));
      }
    }
  }
  ObIvfMacroBlockWriter writer;
  bool writer_opened = false;
  if (OB_FAIL(ret)) {
  } else if (!is_inited_ || quantization_finished_ || raw_write_finished_
      || vector_count <= 0 || center_count <= 0
      || posting_count < vector_count
      || posting_count > INT64_MAX - physical_centers) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF quantization completion state", K(ret),
        K(vector_count), K(center_count), K(posting_count), K_(is_inited),
        K_(quantization_finished), K_(raw_write_finished));
  } else if (FALSE_IT(expected_row_count = physical_centers + posting_count)) {
  } else if (centroid_row_count_ != physical_centers
      || one_bit_row_count_ != posting_count
      || output_row_count_ != expected_row_count
      || sorter_.get_row_count() != expected_row_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF one-bit row count mismatch", K(ret),
        K(expected_row_count), K_(output_row_count),
        K_(centroid_row_count), K_(one_bit_row_count),
        K(vector_count), K(center_count), K(posting_count),
        "sorter_row_count", sorter_.get_row_count());
  } else if (FALSE_IT(ivf_meta_.centroid_count_ = center_count)) {
  } else if (OB_FAIL(sorter_.finish())) {
    LOG_WARN("failed to finish IVF quantization-row sorter", K(ret));
  } else if (OB_FAIL(open_writer_(writer, IVF_OUTPUT_WRITER_IDX))) {
    LOG_WARN("failed to open IVF quantization output writer", K(ret));
  } else if (FALSE_IT(writer_opened = true)) {
  } else if (OB_FAIL(write_sorted_rows_(writer))) {
    LOG_WARN("failed to write sorted IVF quantization rows", K(ret));
  }
  if (writer_opened) {
    const int close_ret = writer.close();
    if (OB_SUCCESS != close_ret) {
      LOG_WARN("failed to close IVF quantization writer", K(close_ret));
      if (OB_SUCC(ret)) {
        ret = close_ret;
      }
    }
  }
  if (OB_SUCC(ret)) {
    ivf_meta_.centroid_count_ = center_count;
    ivf_meta_.vector_count_ = vector_count;
    sorter_.reset(); // QUANT consumed every centroid/posting row; RAW bypasses sorting.
    sorter_inited_ = false;
    quantization_finished_ = true;
    centroid_layout_ = nullptr;
    LOG_INFO("IVF quantization reuse summary", K(vector_count), K(center_count), K(posting_count),
        "reused_postings", ATOMIC_LOAD(&reused_posting_count_),
        "encoded_postings", posting_count - ATOMIC_LOAD(&reused_posting_count_));
  }
  return ret;
}

int ObIVFMergeSSTableWriter::build_macro_seq_window_(
    const ObMacroSeqWindow &data_window,
    const int64_t writer_idx,
    ObMacroSeqWindow &window)
{
  int ret = OB_SUCCESS;
  const int64_t writer_count = OUTPUT_WRITER_COUNT;
  window.reset();
  if (data_window.start_ < 0 || data_window.end_ <= data_window.start_
      || data_window.end_ - data_window.start_ < writer_count
      || writer_idx < 0 || writer_idx >= writer_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF macro sequence window argument", K(ret),
        K(data_window), K(writer_idx));
  } else {
    // The merge ctx owns sequence allocation, including SS overrides. IVF
    // only subdivides its data range, excluding the final index tree slot.
    // Writer 0 owns centers; 1..MAX_QUANT_TASK_COUNT own posting ranges; RAW follows.
    const int64_t window_size = (data_window.end_ - data_window.start_) / writer_count;
    window.start_ = data_window.start_ + writer_idx * window_size;
    window.end_ = writer_idx + 1 == writer_count
        ? data_window.end_ : window.start_ + window_size;
  }
  return ret;
}

int ObIVFMergeSSTableWriter::open_writer_(
    ObIvfMacroBlockWriter &writer,
    const int64_t writer_idx)
{
  int ret = OB_SUCCESS;
  ObMacroSeqParam macro_seq_param;
  ObMacroSeqWindow macro_seq_window;
  ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  ObISSTableObjectCleaner *object_cleaner = nullptr;
  if (!is_inited_ || OB_ISNULL(data_store_desc_)
      || OB_ISNULL(index_builder_) || writer_idx < 0) {
    ret = OB_NOT_INIT;
    LOG_WARN("invalid IVF output writer state", K(ret), K(writer_idx),
        K_(is_inited), KP_(data_store_desc), KP_(index_builder));
  } else if (OB_ISNULL(object_cleaner = index_builder_->get_object_cleaner())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF output object cleaner is null", K(ret), K(writer_idx));
  } else if (OB_FAIL(build_macro_seq_window_(
                 data_macro_seq_window_,
                 writer_idx, macro_seq_window))) {
    LOG_WARN("failed to allocate IVF writer macro sequence window", K(ret));
  } else {
    macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    macro_seq_param.start_ = macro_seq_window.start_;
    macro_seq_param.end_ = macro_seq_window.end_;
    if (OB_FAIL(writer.open(
        row_desc_, *data_store_desc_, writer_idx, macro_seq_param,
        pre_warm_param, *object_cleaner))) {
      LOG_WARN("failed to open IVF output writer", K(ret), K(writer_idx));
    }
  }
  return ret;
}

int ObIVFMergeSSTableWriter::append_macro_row_(
    ObIvfMacroBlockWriter &writer,
    const ObIVFSortItem &item,
    ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const int64_t physical_column_count = row_desc_.payload_col_idx_ + 1;
  if (!is_inited_ || !row_desc_.is_valid()
      || row.get_column_count() < physical_column_count
      || item.cid_ < 0 || item.payload_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF macro output row", K(ret), K(item),
        "column_count", row.get_column_count(), K(physical_column_count),
        K_(is_inited));
  } else {
    row.row_flag_.reset();
    row.mvcc_row_flag_.reset();
    row.storage_datums_[row_desc_.row_type_col_idx_].set_int(item.row_type_);
    row.storage_datums_[row_desc_.centroid_id_col_idx_].set_int(item.cid_);
    row.storage_datums_[row_desc_.vector_id_col_idx_].set_uint(item.vid_);
    row.storage_datums_[row_desc_.trans_version_col_idx_].set_int(
        -snapshot_version_);
    row.storage_datums_[row_desc_.sql_sequence_col_idx_].set_int(0);
    row.storage_datums_[row_desc_.payload_col_idx_].set_string(item.payload_);
    row.count_ = static_cast<int16_t>(physical_column_count);
    row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    row.mvcc_row_flag_.set_last_multi_version_row(true);
    if (OB_FAIL(writer.append_row(row))) {
      LOG_WARN("failed to append sorted IVF output row", K(ret), K(item));
    }
  }
  return ret;
}

int ObIVFMergeSSTableWriter::write_sorted_rows_(
    ObIvfMacroBlockWriter &writer)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator row_allocator(
      ObMemAttr(MTL_ID(), "IVFWriteRows", ObCtxIds::VECTOR_CTX_ID));
  ObDatumRow row;
  const ObIVFSortItem *item = nullptr;
  const int64_t physical_column_count = row_desc_.payload_col_idx_ + 1;
  int64_t previous_row_type = IVF_ROW_TYPE_INVALID;
  int64_t written_row_count = 0;
  int64_t center_self = 0, center_group = 0;
  if (OB_FAIL(row.init(row_allocator, physical_column_count))) {
    LOG_WARN("failed to initialize sorted IVF output row", K(ret));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(sorter_.get_next_item(item))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      }
      LOG_WARN("failed to get sorted IVF output row", K(ret));
    } else if (OB_ISNULL(item)
        || (IVF_ROW_TYPE_CENTROID != item->row_type_
            && IVF_ROW_TYPE_LOW_PRECISION_VECTOR != item->row_type_)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("sorter returned invalid IVF row", K(ret), KPC(item));
    } else if (IVF_ROW_TYPE_CENTROID == item->row_type_
        && ((OB_SUCCESS != ivf_meta_.decode_centroid_key(item->cid_, item->vid_, center_self, center_group))
            || item->payload_.length() != ObIvfCentroidPayload::get_size(vector_dim_))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid IVF centroid row", K(ret), KPC(item));
    } else if (IVF_ROW_TYPE_LOW_PRECISION_VECTOR == item->row_type_
        && item->payload_.length() != one_bit_code_size_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid one-bit IVF posting row", K(ret), KPC(item));
    } else if (IVF_ROW_TYPE_INVALID != previous_row_type
        && previous_row_type != item->row_type_
        && OB_FAIL(writer.flush_micro_block())) {
      LOG_WARN("failed to flush IVF row-type boundary",
          K(ret), K(previous_row_type), "row_type", item->row_type_);
    } else if (OB_FAIL(append_macro_row_(writer, *item, row))) {
      LOG_WARN("failed to append sorted IVF macro row", K(ret), KPC(item));
    } else {
      previous_row_type = item->row_type_;
      ++written_row_count;
    }
  }
  if (OB_SUCC(ret) && written_row_count != output_row_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF writer row count mismatch", K(ret),
        K(written_row_count), K_(output_row_count));
  }
  return ret;
}

int ObIVFMergeSSTableWriter::write_raw_vectors(
    ObIVFVectorReader &reader,
    const ObIArray<ObIVFVectorVID> &all_vids,
    const int64_t vid_begin,
    const int64_t vid_end,
    const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator task_allocator(
      ObMemAttr(MTL_ID(), "IVFRawWrite", ObCtxIds::VECTOR_CTX_ID));
  ObSEArray<float, 16, ObWrapperAllocator> vectors(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(task_allocator));
  ObArenaAllocator row_allocator(
      ObMemAttr(MTL_ID(), "IVFRawRow", ObCtxIds::VECTOR_CTX_ID));
  ObDatumRow row;
  ObIVFSortItem raw_item;
  ObIvfMacroBlockWriter writer;
  int64_t written_row_count = 0;
  bool writer_opened = false;
  const int64_t physical_column_count = row_desc_.payload_col_idx_ + 1;
  if (!is_inited_ || !quantization_finished_ || raw_write_finished_
      || all_vids.empty() || reader.get_dimension() != vector_dim_
      || vid_begin < 0 || vid_end <= vid_begin || vid_end > all_vids.count()
      || task_idx < 0
      || task_idx >= ObIVFMergeProcessor::MAX_RAW_TASK_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF RAW write range", K(ret), K(vid_begin), K(vid_end),
        K(task_idx), "vector_count", all_vids.count(), K_(vector_dim),
        "reader_dim", reader.get_dimension(), K_(is_inited),
        K_(quantization_finished), K_(raw_write_finished));
  }
  for (int64_t i = MAX(1L, vid_begin);
       OB_SUCC(ret) && i < vid_end;
       ++i) {
    if (all_vids.at(i - 1) >= all_vids.at(i)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("IVF raw-vector input is not strictly VID ordered",
          K(ret), K(i), "previous_vid", all_vids.at(i - 1),
          "current_vid", all_vids.at(i));
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("IVF merge failed", K(ret), "operation", "ret");
  } else if (OB_FAIL(open_writer_(writer, MAX_QUANT_TASK_COUNT + task_idx + 1))) {
    LOG_WARN("failed to open IVF RAW output writer", K(ret), K(task_idx),
        K(vid_begin), K(vid_end));
  } else if (FALSE_IT(writer_opened = true)) {
  } else if (OB_FAIL(row.init(row_allocator, physical_column_count))) {
    LOG_WARN("failed to initialize IVF raw output row", K(ret));
  }

  for (int64_t batch_begin = vid_begin;
       OB_SUCC(ret) && batch_begin < vid_end;
       batch_begin += IVF_QUANTIZE_BATCH_SIZE) {
    const int64_t batch_end = MIN(
        vid_end, batch_begin + IVF_QUANTIZE_BATCH_SIZE);
    vectors.reuse();
    if (OB_FAIL(reader.batch_get_by_vid(
        all_vids, batch_begin, batch_end, vectors))) {
      LOG_WARN("failed to read IVF raw-vector batch",
          K(ret), K(batch_begin), K(batch_end));
    } else if (vectors.count()
        != (batch_end - batch_begin) * vector_dim_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected IVF raw-vector batch size", K(ret),
          K(batch_begin), K(batch_end), "value_count", vectors.count());
    }
    for (int64_t i = batch_begin; OB_SUCC(ret) && i < batch_end; ++i) {
      const int64_t local_idx = i - batch_begin;
      raw_item.row_type_ = IVF_ROW_TYPE_RAW_VECTOR;
      raw_item.cid_ = IVF_RAW_VECTOR_CID;
      raw_item.vid_ = all_vids.at(i);
      raw_item.payload_.assign_ptr(
          reinterpret_cast<char *>(&vectors.at(local_idx * vector_dim_)),
          static_cast<int32_t>(vector_dim_ * sizeof(float)));
      if (OB_SUCC(ret) && OB_FAIL(append_macro_row_(writer, raw_item, row))) {
        LOG_WARN("failed to stream IVF raw vector",
            K(ret), K(i), K(all_vids.at(i)));
      } else if (OB_SUCC(ret)) {
        ++written_row_count;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(share::dag_yield())) {
      LOG_WARN("IVF raw-vector write was interrupted",
          K(ret), K(batch_begin), K(batch_end));
    }
  }

  if (OB_SUCC(ret) && written_row_count != vid_end - vid_begin) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF RAW range row count mismatch", K(ret), K(vid_begin),
        K(vid_end), K(written_row_count), K(task_idx));
  }
  if (writer_opened) {
    const int close_ret = writer.close();
    if (OB_SUCCESS != close_ret) {
      LOG_WARN("failed to close IVF RAW range writer", K(close_ret),
          K(vid_begin), K(vid_end), K(task_idx));
      if (OB_SUCC(ret)) {
        ret = close_ret;
      }
    }
  }
  if (OB_SUCC(ret)) {
    (void)ATOMIC_AAF(&raw_vector_row_count_, written_row_count);
    (void)ATOMIC_AAF(&output_row_count_, written_row_count);
  }
  return ret;
}

int ObIVFMergeSSTableWriter::finish_raw_write(
    const int64_t vector_count)
{
  int ret = OB_SUCCESS;
  int64_t expected_row_count = 0;
  const int64_t raw_vector_row_count = ATOMIC_LOAD(&raw_vector_row_count_);
  const int64_t output_row_count = ATOMIC_LOAD(&output_row_count_);
  if (!is_inited_ || !quantization_finished_ || raw_write_finished_
      || vector_count <= 0
      || vector_count > INT64_MAX - centroid_row_count_ - one_bit_row_count_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF RAW write completion state", K(ret), K(vector_count),
        K_(is_inited), K_(quantization_finished), K_(raw_write_finished),
        K_(centroid_row_count), K_(one_bit_row_count));
  } else if (FALSE_IT(expected_row_count =
      centroid_row_count_ + one_bit_row_count_ + vector_count)) {
  } else if (raw_vector_row_count != vector_count
      || output_row_count != expected_row_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF physical row count mismatch", K(ret),
        K(expected_row_count), K(output_row_count), K(raw_vector_row_count),
        K(vector_count), "sorter_row_count", sorter_.get_row_count());
  } else if (OB_ISNULL(index_builder_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("IVF SSTable index builder is null at RAW write completion", K(ret));
  } else if (OB_FAIL(index_builder_->set_ivf_meta(ivf_meta_))) {
    LOG_WARN("failed to publish IVF metadata to SSTable index builder",
        K(ret), K_(ivf_meta));
  } else {
    raw_write_finished_ = true;
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase

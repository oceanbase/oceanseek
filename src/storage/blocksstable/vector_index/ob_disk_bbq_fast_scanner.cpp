/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_disk_bbq_fast_scanner.h"
#include <algorithm>
#include <cmath>
#include "lib/container/ob_array_helper.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector_index/quantization/ob_quantization_computer.h"
#include "storage/blocksstable/encoding/ob_imicro_block_decoder.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace storage;

constexpr int64_t ObDistBBQFastScanBatch::CAPACITY;
constexpr int64_t ObDiskBBQFastScanner::DATA_PREFETCH_DEPTH;
constexpr int64_t ObDiskBBQFastScanner::DATA_RING_CAP;

ObDiskBBQFastScanner::ObDiskBBQFastScanner()
  : sstable_(nullptr),
    read_info_(nullptr),
    access_ctx_(nullptr),
    read_stats_(nullptr),
    row_desc_(),
    quantization_params_(),
    query_allocator_(ObMemAttr(MTL_ID(), "BBQQuery")),
    computer_(),
    fastscan_computer_(),
    code_size_(0),
    decoder_allocator_(ObMemAttr(MTL_ID(), "BBQDecoder")),
    scan_allocator_(ObMemAttr(MTL_ID(), "BBQScan")),
    data_io_allocator_(),
    index_scanner_(),
    index_scan_param_(),
    reader_helper_(),
    micro_reader_(nullptr),
    fastscan_header_(nullptr),
    quantized_row_header_(nullptr),
    current_fastscan_batch_idx_(0),
    current_fastscan_vid_idx_(0),
    data_ring_(),
    data_read_idx_(0),
    data_prefetch_idx_(0),
    current_cid_(-1),
    pre_filter_(nullptr),
    empty_pre_filter_(false),
    use_lower_bound_(true),
    prepared_query_(nullptr),
    query_raw_norm_(0.0F),
    scan_query_(nullptr),
    scan_centroid_(nullptr),
    row_computer_ready_(false),
    current_row_idx_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    current_last_row_idx_(ObIMicroBlockReaderInfo::INVALID_ROW_INDEX),
    row_ids_(),
    vid_values_(),
    vid_datums_(),
    payload_datums_(),
    cell_datas_(),
    decode_columns_(),
    decode_column_params_(),
    decode_datum_infos_(),
    posting_start_datums_(),
    posting_end_datums_(),
    posting_start_key_(),
    posting_end_key_(),
    posting_range_(),
    index_iter_end_(false),
    micro_block_opened_(false),
    scan_opened_(false),
    is_inited_(false)
{
  MEMSET(cell_datas_, 0, sizeof(cell_datas_));
}

ObDiskBBQFastScanner::~ObDiskBBQFastScanner()
{
  reset();
}

void ObDiskBBQFastScanner::reset_scan_state()
{
  if (nullptr != micro_reader_) {
    micro_reader_->reset();
    micro_reader_ = nullptr;
  }
  for (int64_t i = 0; i < DATA_RING_CAP; ++i) {
    data_ring_[i].reset();
  }
  data_read_idx_ = 0;
  data_prefetch_idx_ = 0;
  current_cid_ = -1;
  pre_filter_ = nullptr;
  empty_pre_filter_ = false;
  scan_query_ = nullptr;
  scan_centroid_ = nullptr;
  row_computer_ready_ = false;
  current_row_idx_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  current_last_row_idx_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  fastscan_header_ = nullptr;
  quantized_row_header_ = nullptr;
  current_fastscan_batch_idx_ = 0;
  current_fastscan_vid_idx_ = 0;
  read_stats_ = nullptr;
  index_iter_end_ = false;
  micro_block_opened_ = false;
  scan_opened_ = false;
}

void ObDiskBBQFastScanner::close_scan()
{
  if (nullptr == prepared_query_) { computer_.reset(); }
  fastscan_computer_.reset();
  reset_scan_state();
  index_scanner_.reset();
  index_scan_param_ = ObSSTableIndexScanParam();
  scan_allocator_.reuse();
}

void ObDiskBBQFastScanner::reset()
{
  end_query();
  query_allocator_.reset();
  reader_helper_.reset();
  decoder_allocator_.reset();
  data_io_allocator_.reset();
  scan_allocator_.reset();
  access_ctx_ = nullptr;
  read_info_ = nullptr;
  sstable_ = nullptr;
  row_desc_.reset();
  decode_columns_.reset();
  decode_column_params_.reset();
  decode_datum_infos_.reset();
  quantization_params_ = {};
  posting_range_.reset();
  posting_start_key_.reset();
  posting_end_key_.reset();
  code_size_ = 0;
  is_inited_ = false;
}

int ObDiskBBQFastScanner::begin_query(const float *rotated_query)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  } else if (scan_opened_) {
    ret = OB_STATE_NOT_MATCH;
  } else if (nullptr == rotated_query) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    end_query();
    float squared_norm = 0.0F;
    for (int64_t i = 0; i < quantization_params_.dim_; ++i) {
      squared_norm += rotated_query[i] * rotated_query[i];
    }
    query_raw_norm_ = std::sqrt(squared_norm);
    prepared_query_ = rotated_query;
  }
  return ret;
}

void ObDiskBBQFastScanner::end_query()
{
  prepared_query_ = nullptr;
  query_raw_norm_ = 0.0F;
  close_scan();
  query_allocator_.reuse();
}

int ObDiskBBQFastScanner::init(
    const ObTableIterParam &iter_param,
    const ObIvfSSTableRowDesc &row_desc,
    ObIvfSSTable &sstable,
    ObTableAccessContext &access_ctx)
{
  int ret = OB_SUCCESS;
  const ObIvfSSTableMeta &meta = sstable.get_ivf_meta();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("DiskBBQ fast scanner is already initialized", K(ret));
  } else if (OB_UNLIKELY(!iter_param.is_valid() || !row_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid DiskBBQ fast scanner argument", K(ret), K(iter_param), K(row_desc));
  } else if (OB_UNLIKELY(!access_ctx.is_valid() || access_ctx.tablet_id_ != sstable.get_key().get_tablet_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid DiskBBQ fast scanner access context", K(ret), K(access_ctx), K(sstable));
  } else if (OB_UNLIKELY(!sstable.is_valid() || sstable.no_data_to_read() || !meta.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf sstable is not ready for DiskBBQ fast scan", K(ret), "is_empty", meta.is_empty(), "dim", meta.dim_);
  } else if (OB_UNLIKELY(meta.nbits_ != 1)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported RaBitQ low precision vector code width", K(ret), K(meta.nbits_));
  } else if (OB_FAIL(data_io_allocator_.init(nullptr, OB_MALLOC_MIDDLE_BLOCK_SIZE, ObMemAttr(MTL_ID(), "BBQDataIO")))) {
    LOG_WARN("failed to initialize DiskBBQ data IO allocator", K(ret));
  } else if (OB_FAIL(reader_helper_.init(decoder_allocator_))) {
    LOG_WARN("failed to initialize DiskBBQ micro block reader helper", K(ret));
  } else {
    sstable_ = &sstable;
    read_info_ = iter_param.read_info_;
    access_ctx_ = &access_ctx;
    row_desc_ = row_desc;
    quantization_params_.dim_ = meta.dim_;
    quantization_params_.bits_ = 1;
    if (OB_FAIL(decode_columns_.push_back(row_desc_.vector_id_col_idx_))
        || OB_FAIL(decode_columns_.push_back(row_desc_.payload_col_idx_))
        || OB_FAIL(decode_column_params_.push_back(nullptr))
        || OB_FAIL(decode_column_params_.push_back(nullptr))
        || OB_FAIL(decode_datum_infos_.push_back(ObSqlDatumInfo(vid_datums_, nullptr)))
        || OB_FAIL(decode_datum_infos_.push_back(ObSqlDatumInfo(payload_datums_, nullptr)))) {
      LOG_WARN("failed to initialize DiskBBQ projected columns", K(ret));
    } else if (OB_FAIL(quantization::ObRaBitQQuantizer::calc_code_size(quantization_params_, code_size_))) {
      LOG_WARN("failed to calculate DiskBBQ fast scanner code size", K(ret), K(meta.dim_));
    } else if (OB_FAIL(init_posting_range())) {
      LOG_WARN("failed to initialize DiskBBQ posting range", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObDiskBBQFastScanner::init_posting_range()
{
  int ret = OB_SUCCESS;
  posting_start_datums_[IVF_ROWKEY_COL_TYPE].set_int(IVF_ROW_TYPE_LOW_PRECISION_VECTOR);
  posting_start_datums_[IVF_ROWKEY_COL_CID].set_int(0);
  posting_start_datums_[IVF_ROWKEY_COL_VID].set_min();
  posting_end_datums_[IVF_ROWKEY_COL_TYPE].set_int(IVF_ROW_TYPE_LOW_PRECISION_VECTOR);
  posting_end_datums_[IVF_ROWKEY_COL_CID].set_int(0);
  posting_end_datums_[IVF_ROWKEY_COL_VID].set_max();
  if (OB_FAIL(posting_start_key_.assign(posting_start_datums_, IVF_ROWKEY_COL_COUNT))) {
    LOG_WARN("failed to assign DiskBBQ posting range start key", K(ret));
  } else if (OB_FAIL(posting_end_key_.assign(posting_end_datums_, IVF_ROWKEY_COL_COUNT))) {
    LOG_WARN("failed to assign DiskBBQ posting range end key", K(ret));
  } else {
    posting_range_.set_start_key(posting_start_key_);
    posting_range_.set_end_key(posting_end_key_);
    posting_range_.set_left_closed();
    posting_range_.set_right_closed();
  }
  return ret;
}

void ObDiskBBQFastScanner::set_posting_cid(const int64_t cid)
{
  posting_start_datums_[IVF_ROWKEY_COL_CID].set_int(cid);
  posting_end_datums_[IVF_ROWKEY_COL_CID].set_int(cid);
}

int ObDiskBBQFastScanner::prefetch_one_data_block(
    const ObMicroIndexInfo &index_info,
    ObMicroBlockDataHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  handle.allocator_ = &data_io_allocator_;
  handle.tenant_id_ = MTL_ID();
  handle.effective_tablet_id_ = access_ctx_->tablet_id_;
  ObMicroBlockCacheKey key(MTL_ID(), index_info);
  ObStorageCacheSuite &suite = ObStorageCacheSuite::get_instance();
  const ObMicroBlockData::Type block_type = nullptr != pre_filter_
      ? ObMicroBlockData::IVF_QUANTIZED_ROW_BLOCK : ObMicroBlockData::IVF_FASTSCAN_BLOCK;
  ObDataMicroBlockCache &cache = nullptr != pre_filter_
      ? suite.get_ivf_quantized_row_cache() : suite.get_ivf_fastscan_cache();
  if (OB_UNLIKELY(!index_info.is_data_block() || nullptr == index_info.row_header_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected non-data block in DiskBBQ fast scan", K(ret), K(index_info));
  } else if (OB_FAIL(index_info.row_header_->fill_micro_des_meta(true, handle.des_meta_))) {
    LOG_WARN("failed to retain DiskBBQ micro block deserialize meta", K(ret));
  } else {
    handle.macro_block_id_ = index_info.get_macro_id();
    handle.micro_info_.set(
        index_info.get_block_offset(), index_info.get_block_size(),
        index_info.get_logic_micro_id(), index_info.get_data_checksum());
    ret = cache.get_cache_block(key, handle.cache_handle_);
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(handle.cache_handle_.get_block_data())
          || handle.cache_handle_.get_block_data()->type_ != block_type) {
        ret = OB_INVALID_DATA;
        LOG_WARN("unexpected DiskBBQ vector cache format", K(ret), K(block_type), K(key));
      } else {
        handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_CACHE;
      }
    } else if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get DiskBBQ vector block from cache", K(ret), K(index_info));
    } else {
      const ObIvfFastScanTransformContext transform_ctx(
          quantization_params_.dim_, sstable_->get_ivf_meta().format_version_);
      void *buf = nullptr;
      if (OB_UNLIKELY(!transform_ctx.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid DiskBBQ vector IO context", K(ret), K(transform_ctx));
      } else if (OB_ISNULL(buf = data_io_allocator_.alloc(sizeof(ObIvfVectorBlockIOCallback)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate DiskBBQ vector IO callback", K(ret));
      } else {
        ObIvfVectorBlockIOCallback *callback = new (buf) ObIvfVectorBlockIOCallback(
            data_io_allocator_, true, transform_ctx, block_type);
        ObAsyncSingleMicroBlockIOCallback *io_callback = callback;
        if (OB_FAIL(cache.prefetch(MTL_ID(), index_info.get_macro_id(), index_info,
            access_ctx_->tablet_id_, handle.io_handle_, io_callback, true))) {
          LOG_WARN("failed to prefetch DiskBBQ vector micro block", K(ret), K(index_info));
        } else {
          handle.block_state_ = ObSSTableMicroBlockState::IN_BLOCK_IO;
        }
        if (OB_NOT_NULL(io_callback)) {
          callback->~ObIvfVectorBlockIOCallback();
          data_io_allocator_.free(callback);
        }
      }
    }
  }
  return ret;
}

int ObDiskBBQFastScanner::try_prefetch_data_blocks()
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && !index_iter_end_ && !is_prefetch_window_full()) {
    ObMicroIndexInfo index_info;
    const int scan_ret = index_scanner_.get_next(index_info);
    if (OB_ITER_END == scan_ret) {
      index_iter_end_ = true;
    } else if (OB_SUCCESS != scan_ret) {
      ret = scan_ret;
      LOG_WARN("failed to get next DiskBBQ data micro block index", K(ret));
    } else {
      int64_t block_row_type = 0;
      if (OB_FAIL(index_info.endkey_.get_column_int(
          row_desc_.row_type_col_idx_, block_row_type))) {
        LOG_WARN("failed to read DiskBBQ data block row type", K(ret), K(index_info));
      } else if (IVF_ROW_TYPE_LOW_PRECISION_VECTOR != block_row_type) {
        // Range lookup may return an adjacent boundary micro block. IVF row
        // types never share a micro block, so it cannot contain posting rows.
        LOG_TRACE("skip non-posting DiskBBQ boundary block",
            K(block_row_type), K(index_info));
      } else {
        DataPrefetchItem &item = next_prefetch_item();
        item.reset();
        item.is_left_border_ = index_info.is_left_border();
        item.is_right_border_ = index_info.is_right_border();
        // Issue data IO while index_info is still backed by the current pinned leaf
        // block. Only the fully owned handle and border flags enter the ring.
        if (OB_FAIL(prefetch_one_data_block(index_info, item.data_handle_))) {
          LOG_WARN("failed to prefetch DiskBBQ data block", K(ret), K(index_info));
          item.reset();
        } else {
          ++data_prefetch_idx_;
        }
      }
    }
  }
  return ret;
}

int ObDiskBBQFastScanner::inner_open_scan(
    const int64_t cid,
    ObIvfSSTableReadStats *read_stats)
{
  int ret = OB_SUCCESS;
  bool scan_state_mutated = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("DiskBBQ fast scanner is not initialized", K(ret), K(cid));
  } else if (OB_UNLIKELY(scan_opened_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("DiskBBQ fast scan is already open", K(ret), K(cid), K_(current_cid));
  } else {
    scan_state_mutated = true;
    if (nullptr != read_stats) {
      ++read_stats->low_precision_vector_scan_count_;
      if (-1 == read_stats->first_low_precision_vector_scan_cid_) {
        read_stats->first_low_precision_vector_scan_cid_ = cid;
      }
    }
    {
      ObOpProfile<ObMetric> *profile = get_current_profile();
      if (nullptr != profile) {
        int ret = OB_SUCCESS; // Keep diagnostic errors local.
        INC_METRIC_VAL(ObMetricId::VEC_IVF_LOW_PRECISION_VECTOR_SCAN_COUNT, 1);
        if (cid >= 0 && nullptr == profile->get_metric(ObMetricId::VEC_IVF_FIRST_LOW_PRECISION_VECTOR_SCAN_CID)) {
          SET_METRIC_VAL(ObMetricId::VEC_IVF_FIRST_LOW_PRECISION_VECTOR_SCAN_CID, cid);
        }
      }
    }
    read_stats_ = read_stats;
    ObQueryFlag query_flag = access_ctx_->query_flag_;
    query_flag.scan_order_ = ObQueryFlag::Forward;
    set_posting_cid(cid);
    if (OB_FAIL(index_scan_param_.init(*read_info_, query_flag, access_ctx_->tablet_id_, ObSSTableIndexScanParam::LEAF))) {
      LOG_WARN("failed to initialize DiskBBQ index scan parameter", K(ret), K(cid));
    } else if (OB_FAIL(index_scanner_.init(posting_range_, index_scan_param_, *sstable_, scan_allocator_))) {
      LOG_WARN("failed to initialize DiskBBQ index scanner", K(ret), K(cid), K(posting_range_));
    } else {
      current_cid_ = cid;
      if (OB_FAIL(try_prefetch_data_blocks())) {
        LOG_WARN("failed to prime DiskBBQ data block prefetch", K(ret), K(cid));
      }
    }
  }
  if (OB_FAIL(ret) && scan_state_mutated) {
    close_scan();
  }
  return ret;
}

int ObDiskBBQFastScanner::prepare_row_computer()
{
  int ret = OB_SUCCESS;
  // A query-scoped computer can still contain the previous CID's centroid.
  if (!row_computer_ready_) {
    const auto metric = static_cast<share::ObVectorIndexDistAlgorithm>(sstable_->get_ivf_meta().dist_type_);
    if (nullptr == prepared_query_) {
      ret = computer_.init(scan_allocator_, quantization::ObQuantizationType::RABITQ,
          metric, quantization_params_, scan_query_, scan_centroid_);
    } else if (computer_.is_inited()) {
      ret = computer_.reprepare_query(scan_query_, scan_centroid_, &query_raw_norm_);
    } else {
      ret = computer_.init(query_allocator_, quantization::ObQuantizationType::RABITQ,
          metric, quantization_params_, scan_query_, scan_centroid_, &query_raw_norm_);
    }
    if (OB_SUCC(ret)) {
      row_computer_ready_ = true;
    }
  }
  return ret;
}

int ObDiskBBQFastScanner::open_scan(
    const int64_t cid,
    ObIvfSSTableReadStats *read_stats,
    const float *rotated_query,
    const float *rotated_centroid,
    const ObIvfPreFilter *pre_filter,
    const bool use_lower_bound)
{
  int ret = OB_SUCCESS;
  bool need_cleanup = false;
  // Validate the filter and detect an empty signed VID range before submitting IO.
  int64_t min_vid = INT64_MIN;
  int64_t max_vid = INT64_MAX;
  if (OB_UNLIKELY(cid < 0) || OB_ISNULL(rotated_query) || OB_ISNULL(rotated_centroid)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid DiskBBQ approximate scan vectors", K(ret), K(cid),
        KP(rotated_query), KP(rotated_centroid));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("DiskBBQ fast scanner is not initialized", K(ret), K(cid));
  } else if (OB_UNLIKELY(scan_opened_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("DiskBBQ fast scan is already open", K(ret), K(cid), K_(current_cid));
  } else if (nullptr != prepared_query_ && rotated_query != prepared_query_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("DiskBBQ scan query differs from the explicit query scope", K(ret));
  } else if (nullptr != pre_filter && OB_FAIL(pre_filter->get_vid_range(min_vid, max_vid))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      empty_pre_filter_ = true;
      current_cid_ = cid;
      scan_opened_ = true;
    } else {
      LOG_WARN("failed to get DiskBBQ pre-filter VID range", K(ret), K(cid));
    }
  } else if (FALSE_IT(pre_filter_ = pre_filter)) {
  } else if (OB_FAIL(inner_open_scan(cid, read_stats))) {
    LOG_WARN("failed to open DiskBBQ approximate scan", K(ret), K(cid));
  } else {
    need_cleanup = true;
    use_lower_bound_ = use_lower_bound;
    scan_query_ = rotated_query;
    scan_centroid_ = rotated_centroid;
    const auto metric = static_cast<share::ObVectorIndexDistAlgorithm>(sstable_->get_ivf_meta().dist_type_);
    if (nullptr != pre_filter_) {
      if (OB_FAIL(prepare_row_computer())) {
        LOG_WARN("failed to prepare DiskBBQ row computer", K(ret), K(cid));
      }
    } else if (OB_FAIL(fastscan_computer_.init(scan_allocator_,
        metric, quantization_params_, rotated_query, rotated_centroid))) {
      LOG_WARN("failed to initialize cached DiskBBQ FastScan computer", K(ret), K(cid));
    }
    if (OB_SUCC(ret)) {
      scan_opened_ = true;
    }
  }
  if (OB_FAIL(ret) && need_cleanup) {
    close_scan();
  }
  return ret;
}

int ObDiskBBQFastScanner::open_next_micro_block(bool &is_iter_end)
{
  int ret = OB_SUCCESS;
  // Synchronous fallback deep-copies into the data handle's allocator.
  ObMacroBlockReader macro_reader;
  is_iter_end = false;
  if (micro_block_opened_) {
    if (nullptr != micro_reader_) {
      micro_reader_->reset();
      micro_reader_ = nullptr;
    }
    current_data_item().reset();
    ++data_read_idx_;
    micro_block_opened_ = false;
    fastscan_header_ = nullptr;
    quantized_row_header_ = nullptr;
    current_fastscan_batch_idx_ = 0;
    current_fastscan_vid_idx_ = 0;
  }

  while (OB_SUCC(ret) && !micro_block_opened_) {
    if (OB_FAIL(try_prefetch_data_blocks())) {
      LOG_WARN("failed to continue DiskBBQ data block prefetch", K(ret));
    } else if (!has_prefetched_item()) {
      if (OB_UNLIKELY(!index_iter_end_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("DiskBBQ prefetch ring is unexpectedly empty", K(ret));
      } else {
        is_iter_end = true;
      }
      break;
    } else {
      DataPrefetchItem &item = current_data_item();
      ObMicroBlockData block_data;
      int64_t begin_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
      int64_t end_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
      if (OB_FAIL(item.data_handle_.get_micro_block_data(&macro_reader, block_data, true))) {
        LOG_WARN("failed to materialize DiskBBQ data micro block", K(ret), K(item.data_handle_));
      } else if (ObMicroBlockData::IVF_FASTSCAN_BLOCK == block_data.type_) {
        const ObIvfFastScanBlockHeader *header =
            reinterpret_cast<const ObIvfFastScanBlockHeader *>(block_data.get_buf());
        if (OB_ISNULL(header) || block_data.get_buf_size() < sizeof(*header)
            || nullptr != pre_filter_
            || OB_UNLIKELY(!header->is_valid(block_data.get_buf_size()))
            || OB_UNLIKELY(header->dim_ != quantization_params_.dim_)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid cached DiskBBQ FastScan block",
              K(ret), K(block_data), "dim", quantization_params_.dim_,
              "bits", quantization_params_.bits_);
        } else {
          fastscan_header_ = header;
          current_fastscan_batch_idx_ = 0;
          current_fastscan_vid_idx_ = 0;
          current_row_idx_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
          current_last_row_idx_ = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
          micro_block_opened_ = true;
        }
      } else if (ObMicroBlockData::IVF_QUANTIZED_ROW_BLOCK == block_data.type_) {
        const ObIvfQuantizedRowBlockHeader *header =
            reinterpret_cast<const ObIvfQuantizedRowBlockHeader *>(block_data.get_buf());
        if (OB_ISNULL(header) || block_data.get_buf_size() < sizeof(*header)
            || nullptr == pre_filter_ || !header->is_valid(block_data.get_buf_size())
            || header->dim_ != quantization_params_.dim_ || header->code_size_ != code_size_) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid cached DiskBBQ quantized row block", K(ret), K(block_data));
        } else {
          const int64_t *cids = header->cids();
          begin_idx = std::lower_bound(cids, cids + header->row_count_, current_cid_) - cids;
          end_idx = std::upper_bound(cids + begin_idx, cids + header->row_count_, current_cid_) - cids - 1;
          if (end_idx < begin_idx) {
            item.reset();
            ++data_read_idx_;
          } else {
            quantized_row_header_ = header;
            current_row_idx_ = begin_idx;
            current_last_row_idx_ = end_idx;
            micro_block_opened_ = true;
          }
        }
      } else if (ObMicroBlockData::DATA_BLOCK != block_data.type_) {
        ret = OB_INVALID_DATA;
        LOG_WARN("unexpected DiskBBQ cached block type", K(ret), K(block_data));
      } else if (OB_ISNULL(block_data.get_micro_header())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("DiskBBQ data micro block has no valid header", K(ret), K(block_data));
      } else if (OB_FAIL(reader_helper_.get_reader(*block_data.get_micro_header(), micro_reader_))) {
        LOG_WARN("failed to get DiskBBQ micro block decoder", K(ret), K(block_data));
      } else if (OB_FAIL(micro_reader_->init(block_data, *read_info_))) {
        LOG_WARN("failed to initialize DiskBBQ micro block decoder", K(ret), K(block_data));
      } else if (OB_FAIL(micro_reader_->locate_range(posting_range_, item.is_left_border_, item.is_right_border_, begin_idx, end_idx))) {
        if (OB_BEYOND_THE_RANGE == ret) {
          ret = OB_SUCCESS;
          micro_reader_->reset();
          micro_reader_ = nullptr;
          item.reset();
          ++data_read_idx_;
        } else {
          LOG_WARN("failed to locate DiskBBQ range in data micro block", K(ret),
              K(posting_range_), K(item.is_left_border_), K(item.is_right_border_));
        }
      } else if (begin_idx > 0 && end_idx == begin_idx - 1) {
        // The requested CID may fall into a rowkey gap inside a boundary
        // micro block. locate_range() represents that valid empty intersection
        // as [begin_idx, begin_idx - 1] with OB_SUCCESS.
        micro_reader_->reset();
        micro_reader_ = nullptr;
        item.reset();
        ++data_read_idx_;
      } else if (OB_UNLIKELY(begin_idx < 0 || end_idx < begin_idx)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid DiskBBQ row range in data micro block", K(ret), K(begin_idx), K(end_idx));
      } else if (OB_FAIL(prepare_row_computer())) {
        // Vector-cache IO can fall back to a synchronous ordinary DATA_BLOCK read.
        LOG_WARN("failed to prepare DiskBBQ row computer for ordinary block", K(ret), K_(current_cid));
      } else {
        current_row_idx_ = begin_idx;
        current_last_row_idx_ = end_idx;
        micro_block_opened_ = true;
      }
    }
  }
  return ret;
}

bool ObDiskBBQFastScanner::is_current_micro_block_exhausted() const
{
  return nullptr != fastscan_header_
      ? current_fastscan_batch_idx_ >= fastscan_header_->batch_count_
      : current_row_idx_ > current_last_row_idx_;
}

bool ObDiskBBQFastScanner::seek_current_fastscan_batch(const int64_t cid)
{
  bool found = false;
  if (nullptr != fastscan_header_) {
    const int64_t *batch_cids = reinterpret_cast<const int64_t *>(
        reinterpret_cast<const char *>(fastscan_header_)
            + fastscan_header_->batch_cids_offset_);
    const int32_t *batch_row_counts = reinterpret_cast<const int32_t *>(
        reinterpret_cast<const char *>(fastscan_header_)
            + fastscan_header_->batch_row_counts_offset_);
    while (current_fastscan_batch_idx_ < fastscan_header_->batch_count_
        && batch_cids[current_fastscan_batch_idx_] != cid) {
      current_fastscan_vid_idx_ += batch_row_counts[current_fastscan_batch_idx_];
      ++current_fastscan_batch_idx_;
    }
    found = current_fastscan_batch_idx_ < fastscan_header_->batch_count_;
  }
  return found;
}

int ObDiskBBQFastScanner::decode_current_fastscan_batch(
    ObDistBBQFastScanBatch &batch,
    bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  if (OB_ISNULL(fastscan_header_)
      || OB_UNLIKELY(current_fastscan_batch_idx_ < 0
          || current_fastscan_batch_idx_ >= fastscan_header_->batch_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid current DiskBBQ FastScan batch",
        K(ret), KP_(fastscan_header), K_(current_fastscan_batch_idx));
  } else {
    const char *block_buf = reinterpret_cast<const char *>(fastscan_header_);
    const int64_t *batch_cids = reinterpret_cast<const int64_t *>(
        block_buf + fastscan_header_->batch_cids_offset_);
    const int32_t *batch_row_counts = reinterpret_cast<const int32_t *>(
        block_buf + fastscan_header_->batch_row_counts_offset_);
    const int64_t *vids = reinterpret_cast<const int64_t *>(
        block_buf + fastscan_header_->vids_offset_);
    const int64_t batch_idx = current_fastscan_batch_idx_;
    const int64_t row_count = batch_row_counts[batch_idx];
    if (batch_cids[batch_idx] == current_cid_) {
      const int64_t padded_row_idx = batch_idx * ObDistBBQFastScanBatch::CAPACITY;
      const int64_t packed_batch_size = static_cast<int64_t>(fastscan_header_->code_bytes_)
          * ObDistBBQFastScanBatch::CAPACITY;
      const uint8_t *packed_codes = reinterpret_cast<const uint8_t *>(
          block_buf + fastscan_header_->codes_offset_ + batch_idx * packed_batch_size);
      const float *residual_norms = reinterpret_cast<const float *>(
          block_buf + fastscan_header_->residual_norms_offset_) + padded_row_idx;
      const float *corrections = reinterpret_cast<const float *>(
          block_buf + fastscan_header_->corrections_offset_) + padded_row_idx;
      const float *raw_norms = reinterpret_cast<const float *>(
          block_buf + fastscan_header_->raw_norms_offset_) + padded_row_idx;
      float distances[ObDistBBQFastScanBatch::CAPACITY];
      // ObQuantizationBuffer is shared by read and write APIs. The FastScan
      // computer only reads this immutable cache payload.
      if (use_lower_bound_) {
        ret = fastscan_computer_.compute_lower_bounds_batch(
            {const_cast<uint8_t *>(packed_codes), packed_batch_size},
            residual_norms, corrections, raw_norms, row_count, distances);
      } else {
        ret = fastscan_computer_.compute_batch(
            {const_cast<uint8_t *>(packed_codes), packed_batch_size},
            residual_norms, corrections, raw_norms, row_count, distances);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to compute cached DiskBBQ FastScan batch",
            K(ret), K_(current_cid), K(batch_idx), K(row_count));
      } else if (OB_FAIL(batch.add_batch(current_cid_,
          vids + current_fastscan_vid_idx_, distances, row_count))) {
        LOG_WARN("failed to append cached DiskBBQ FastScan result batch",
            K(ret), K(batch_idx), K(row_count), "batch_count", batch.count());
      }
      if (OB_SUCC(ret)) {
        found = true;
        if (nullptr != read_stats_) {
          read_stats_->low_precision_vector_count_ =
              row_count > INT64_MAX - read_stats_->low_precision_vector_count_
              ? INT64_MAX : read_stats_->low_precision_vector_count_ + row_count;
        }
        {
          ObOpProfile<ObMetric> *profile = get_current_profile();
          if (nullptr != profile) {
            int ret = OB_SUCCESS; // Keep diagnostic errors local.
            INC_METRIC_VAL(ObMetricId::VEC_IVF_LOW_PRECISION_VECTOR_COUNT, row_count);
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      current_fastscan_vid_idx_ += row_count;
      ++current_fastscan_batch_idx_;
    }
  }
  return ret;
}

int ObDiskBBQFastScanner::decode_column(
    const int32_t column, const int64_t row_count, ObDatum *datums)
{
  int ret = OB_SUCCESS;
  if (ObIMicroBlockReader::Decoder == micro_reader_->get_type()
      || ObIMicroBlockReader::CSDecoder == micro_reader_->get_type()) {
    int32_t col = column;
    const share::schema::ObColumnParam *param = nullptr;
    ObSqlDatumInfo datum_info(datums, nullptr);
    ObArrayHelper<int32_t> columns(1, &col, 1);
    ObArrayHelper<const share::schema::ObColumnParam *> params(1, &param, 1);
    ObArrayHelper<ObSqlDatumInfo> infos(1, &datum_info, 1);
    // Row-encoding decoders may reuse these pointers. The payload pass uses
    // compacted row IDs, so pointers from the VID pass must not be retained.
    MEMSET(cell_datas_, 0, sizeof(cell_datas_));
    ret = static_cast<ObIMicroBlockDecoder *>(micro_reader_)->get_rows(
        columns, params, false, row_ids_, cell_datas_, row_count, infos);
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
      ObStorageDatum datum;
      if (OB_FAIL(micro_reader_->get_raw_column_datum(column, row_ids_[i], datum))) {
        LOG_WARN("failed to decode DiskBBQ PRE column", K(ret), K(column), K(row_ids_[i]));
      } else if (column == row_desc_.vector_id_col_idx_) {
        if (OB_UNLIKELY(datum.is_null() || datum.len_ != sizeof(int64_t))) {
          ret = OB_INVALID_DATA;
        } else {
          vid_values_[i] = datum.get_int();
          datums[i].ptr_ = reinterpret_cast<const char *>(&vid_values_[i]);
          datums[i].pack_ = sizeof(int64_t);
        }
      } else {
        static_cast<ObDatum &>(datums[i]) = datum;
      }
    }
  }
  return ret;
}

int ObDiskBBQFastScanner::decode_filtered_rows(
    const int64_t row_cap,
    int64_t &selected_count)
{
  int ret = OB_SUCCESS;
  selected_count = 0;
  if (nullptr != quantized_row_header_) {
    for (int64_t i = 0; i < row_cap; ++i) {
      vid_values_[i] = quantized_row_header_->vids()[row_ids_[i]];
    }
  } else if (OB_FAIL(decode_column(row_desc_.vector_id_col_idx_, row_cap, vid_datums_))) {
    LOG_WARN("failed to decode DiskBBQ PRE VIDs", K(ret), K(row_cap));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    const ObDatum &vid = vid_datums_[i];
    if (OB_UNLIKELY(vid.is_null() || vid.len_ != sizeof(int64_t))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid DiskBBQ PRE VID", K(ret), K(i));
    } else if (pre_filter_->contains(vid.get_int())) {
      // Copy values before a second get_rows() reuses decoder-owned memory.
      vid_values_[selected_count] = vid.get_int();
      row_ids_[selected_count] = row_ids_[i];
      payload_datums_[selected_count].set_null();
      ++selected_count;
    }
  }
  if (OB_SUCC(ret) && selected_count > 0) {
    if (nullptr != quantized_row_header_) {
      for (int64_t i = 0; i < selected_count; ++i) {
        payload_datums_[i].ptr_ = quantized_row_header_->payload(row_ids_[i]);
        payload_datums_[i].pack_ = code_size_;
      }
    } else if (OB_FAIL(decode_column(row_desc_.payload_col_idx_, selected_count, payload_datums_))) {
      LOG_WARN("failed to decode matching DiskBBQ PRE payloads", K(ret), K(selected_count));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < selected_count; ++i) {
      vid_datums_[i].ptr_ = reinterpret_cast<const char *>(&vid_values_[i]);
      vid_datums_[i].pack_ = sizeof(int64_t);
    }
  }
  return ret;
}

int ObDiskBBQFastScanner::decode_current_rows(
    const int64_t row_cap,
    ObDistBBQFastScanBatch &batch)
{
  int ret = OB_SUCCESS;
  int64_t decoded_count = row_cap;
  if (OB_UNLIKELY((nullptr == micro_reader_ && nullptr == quantized_row_header_) || row_cap <= 0
      || row_cap > ObDistBBQFastScanBatch::CAPACITY - batch.count()
      || current_row_idx_ + row_cap - 1 > current_last_row_idx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid DiskBBQ micro block decode request", K(ret), K_(current_cid), K(row_cap),
        K_(current_row_idx), K_(current_last_row_idx), "batch_count", batch.count());
  }

  if (OB_SUCC(ret) && nullptr != quantized_row_header_) {
    return decode_cached_pre_rows(row_cap, batch);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
    row_ids_[i] = static_cast<int32_t>(current_row_idx_ + i);
    vid_datums_[i].ptr_ = reinterpret_cast<const char *>(&vid_values_[i]);
    vid_datums_[i].pack_ = sizeof(vid_values_[i]);
    payload_datums_[i].set_null();
    cell_datas_[i] = nullptr;
  }

  if (OB_FAIL(ret)) {
  } else if (nullptr != pre_filter_) {
    if (OB_FAIL(decode_filtered_rows(row_cap, decoded_count))) {
      LOG_WARN("failed to decode DiskBBQ PRE rows", K(ret), K(row_cap));
    }
  } else if ((ObIMicroBlockReader::Decoder == micro_reader_->get_type()
          || ObIMicroBlockReader::CSDecoder == micro_reader_->get_type())) {
    if (OB_FAIL(static_cast<ObIMicroBlockDecoder *>(micro_reader_)->get_rows(
        decode_columns_, decode_column_params_, false, row_ids_, cell_datas_,
        row_cap, decode_datum_infos_))) {
      LOG_WARN("failed to batch decode DiskBBQ projected columns", K(ret), K(row_cap));
    }
  } else {
    // Compatibility fallback for flat row stores. DiskBBQ SSTables normally use
    // CS_ENCODING_ROW_STORE, so this path is not expected on the hot path.
    for (int64_t i = 0; OB_SUCC(ret) && i < row_cap; ++i) {
      ObStorageDatum vid;
      ObStorageDatum payload;
      if (OB_FAIL(micro_reader_->get_raw_column_datum(
          row_desc_.vector_id_col_idx_, row_ids_[i], vid))) {
        LOG_WARN("failed to decode DiskBBQ vector id", K(ret), K(row_ids_[i]));
      } else if (OB_FAIL(micro_reader_->get_raw_column_datum(
          row_desc_.payload_col_idx_, row_ids_[i], payload))) {
        LOG_WARN("failed to decode DiskBBQ payload", K(ret), K(row_ids_[i]));
      } else if (OB_UNLIKELY(vid.is_null() || vid.len_ != sizeof(int64_t))) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid DiskBBQ vector id", K(ret), K(row_ids_[i]),
            "vid_null", vid.is_null(), "vid_len", vid.len_);
      } else {
        // Flat readers materialize fixed-width values in ObStorageDatum's local
        // buffer. Keep vid_datums_ pointing at the scanner-owned values because
        // shallow-copying vid would leave it referring to this loop-local datum.
        vid_datums_[i].pack_ = vid.pack_;
        vid_values_[i] = vid.get_int();
        static_cast<ObDatum &>(payload_datums_[i]) = payload;
      }
    }
  }

  quantization::ObQuantizationBuffer codes[ObDistBBQFastScanBatch::CAPACITY];
  float distances[ObDistBBQFastScanBatch::CAPACITY];
  int64_t evaluated_rows = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < decoded_count; ++i) {
    const ObDatum &vid = vid_datums_[i];
    const ObDatum &payload = payload_datums_[i];
    if (vid.is_null() || payload.is_null() || vid.len_ != sizeof(int64_t)
        || OB_ISNULL(payload.ptr_) || payload.len_ != code_size_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid projected DiskBBQ vector row", K(ret), K_(current_cid), K(i));
    } else {
      codes[i] = {reinterpret_cast<uint8_t *>(const_cast<char *>(payload.ptr_)), payload.len_};
    }
  }
  if (OB_SUCC(ret) && decoded_count > 0) {
    if (use_lower_bound_) {
      ret = computer_.compute_lower_bounds_batch(codes, decoded_count, distances);
    } else {
      ret = computer_.compute_batch(codes, decoded_count, distances);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to compute DiskBBQ row batch", K(ret), K_(current_cid), K(decoded_count));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < decoded_count; ++i) {
        ObIvfKnnEntry candidate;
        candidate.cid_ = current_cid_;
        candidate.vid_ = vid_datums_[i].get_int();
        candidate.distance_ = distances[i];
        if (OB_SUCC(ret = batch.add(candidate))) { ++evaluated_rows; }
      }
    }
  }
  if (OB_SUCC(ret)) { finish_row_batch(row_cap, evaluated_rows, batch); }
  return ret;
}

int ObDiskBBQFastScanner::decode_cached_pre_rows(const int64_t row_cap, ObDistBBQFastScanBatch &batch)
{
  int ret = OB_SUCCESS;
  int64_t selected = 0;
  quantization::ObQuantizationBuffer codes[ObDistBBQFastScanBatch::CAPACITY];
  float distances[ObDistBBQFastScanBatch::CAPACITY];
  // open_next_micro_block validated the immutable layout and pinned its cache
  // handle. Keep the physical batch boundary and borrow only matching payloads.
  const int64_t *vids = quantized_row_header_->vids() + current_row_idx_;
  if (OB_ISNULL(pre_filter_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    for (int64_t i = 0; i < row_cap; ++i) {
      if (pre_filter_->contains(vids[i])) {
        vid_values_[selected] = vids[i];
        codes[selected++] = {reinterpret_cast<uint8_t *>(const_cast<char *>(
            quantized_row_header_->payload(current_row_idx_ + i))), code_size_};
      }
    }
    if (selected > 0) {
      if (use_lower_bound_) {
        ret = computer_.compute_lower_bounds_batch(codes, selected, distances);
      } else {
        ret = computer_.compute_batch(codes, selected, distances);
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to compute cached DiskBBQ PRE rows", K(ret), K(selected));
      } else if (OB_FAIL(batch.add_batch(current_cid_, vid_values_, distances, selected))) {
        LOG_WARN("failed to append cached DiskBBQ PRE rows", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) { finish_row_batch(row_cap, selected, batch); }
  return ret;
}

void ObDiskBBQFastScanner::finish_row_batch(const int64_t row_cap,
                                           const int64_t evaluated_rows,
                                           ObDistBBQFastScanBatch &batch)
{
  current_row_idx_ += row_cap;
  if (nullptr != pre_filter_ && evaluated_rows > 0) {
    batch.add_filtered_count(row_cap - evaluated_rows);
  }
  if (nullptr != read_stats_) {
    read_stats_->low_precision_vector_count_ =
        evaluated_rows > INT64_MAX - read_stats_->low_precision_vector_count_
        ? INT64_MAX : read_stats_->low_precision_vector_count_ + evaluated_rows;
  }
  {
    ObOpProfile<ObMetric> *profile = get_current_profile();
    if (nullptr != profile) {
      int ret = OB_SUCCESS; // Keep diagnostic errors local.
      INC_METRIC_VAL(ObMetricId::VEC_IVF_LOW_PRECISION_VECTOR_COUNT, evaluated_rows);
    }
  }
}

int ObDiskBBQFastScanner::get_next_batch(
    ObDistBBQFastScanBatch &batch,
    bool &is_iter_end)
{
  int ret = OB_SUCCESS;
  batch.reset();
  is_iter_end = false;
  if (OB_UNLIKELY(!is_inited_ || !scan_opened_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("DiskBBQ posting scan is not open", K(ret), K_(current_cid));
  }

  if (OB_SUCC(ret) && empty_pre_filter_) {
    is_iter_end = true;
  }

  while (OB_SUCC(ret) && batch.count() < ObDistBBQFastScanBatch::CAPACITY && !is_iter_end) {
    if (!micro_block_opened_ || is_current_micro_block_exhausted()) {
      if (OB_FAIL(open_next_micro_block(is_iter_end))) {
        LOG_WARN("failed to open next DiskBBQ data micro block", K(ret), K_(current_cid));
      }
    }
    if (OB_SUCC(ret) && !is_iter_end) {
      if (nullptr != fastscan_header_) {
        if (batch.count() > 0) {
          break;
        } else {
          bool found = false;
          if (!seek_current_fastscan_batch(current_cid_)) {
          } else if (OB_FAIL(decode_current_fastscan_batch(batch, found))) {
            LOG_WARN("failed to decode cached DiskBBQ FastScan batch", K(ret), K_(current_cid));
          } else if (found) {
            bool has_next = seek_current_fastscan_batch(current_cid_);
            while (OB_SUCC(ret) && !has_next && !is_iter_end) {
              if (OB_FAIL(open_next_micro_block(is_iter_end))) {
                LOG_WARN("failed to look ahead after cached DiskBBQ FastScan batch",
                    K(ret), K_(current_cid));
              } else if (!is_iter_end) {
                has_next = nullptr == fastscan_header_
                    || seek_current_fastscan_batch(current_cid_);
              }
            }
            break;
          }
        }
      } else {
        const int64_t row_cap = MIN(current_last_row_idx_ - current_row_idx_ + 1,
            ObDistBBQFastScanBatch::CAPACITY - batch.count());
        if (OB_FAIL(decode_current_rows(row_cap, batch))) {
          LOG_WARN("failed to decode DiskBBQ approximate batch", K(ret), K_(current_cid), K(row_cap));
        } else if (nullptr != pre_filter_ && batch.count() > 0) {
          // Return one nonempty physical batch, just like FastScan. Filling
          // 32 filtered candidates would scan far past the previous budget.
          while (OB_SUCC(ret) && is_current_micro_block_exhausted() && !is_iter_end) {
            if (OB_FAIL(open_next_micro_block(is_iter_end))) {
              LOG_WARN("failed to look ahead after DiskBBQ PRE batch", K(ret));
            }
          }
          break;
        }
      }
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase

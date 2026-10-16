/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_iterator.h"

#include <algorithm>
#include "lib/compress/ob_compressor_pool.h"
#include "share/config/ob_server_config.h"
#include "share/ob_server_struct.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_decoder.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_reader_helper.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/ob_storage_datum.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

ObInvIndexBlockMicroIterator::ObInvIndexBlockMicroIterator()
  : data_buf_(nullptr),
    data_size_(0),
    posting_entries_(OB_MALLOC_NORMAL_BLOCK_SIZE,
        ModulePageAllocator(lib::ObLabel("InvIdxMicroIt"), MTL_ID())),
    value_entries_(OB_MALLOC_NORMAL_BLOCK_SIZE,
        ModulePageAllocator(lib::ObLabel("InvIdxMicroIt"), MTL_ID())),
    index_alloc_("InvIdxMicroIt", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    index_entries_(OB_MALLOC_NORMAL_BLOCK_SIZE,
        ModulePageAllocator(lib::ObLabel("InvIdxMicroIt"), MTL_ID())),
    datum_utils_(nullptr),
    curr_index_pos_(-1),
    is_opened_(false),
    macro_handle_(),
    allocator_("IBMI_IOUB", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    micro_block_(),
    read_info_(nullptr),
    range_(),
    compressor_type_(common::INVALID_COMPRESSOR),
    is_inited_(false)
{
}

ObInvIndexBlockMicroIterator::~ObInvIndexBlockMicroIterator()
{
  reset();
}

int ObInvIndexBlockMicroIterator::init(
    const ObMacroBlockDesc &macro_desc,
    const ObITableReadInfo &read_info,
    const ObInvSSTable *sstable)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret));
  } else if (OB_UNLIKELY(!read_info.is_valid() || !macro_desc.is_valid()
                         || OB_ISNULL(sstable))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected read info / macro desc / sstable",
        K(ret), K(read_info), K(macro_desc), KP(sstable));
  } else {
    range_ = macro_desc.range_;
    read_info_ = &read_info;

    // 异步读取宏块（对齐 compaction 版本）
    ObStorageObjectReadInfo io_read_info;
    io_read_info.offset_ = sstable->get_macro_offset();
    io_read_info.size_ = sstable->get_macro_read_size();
    io_read_info.io_desc_.set_mode(ObIOMode::READ);
    io_read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    io_read_info.io_desc_.set_sys_module_id(ObIOModule::INDEX_BLOCK_MICRO_ITER_IO);
    io_read_info.io_timeout_ms_ = std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);
    io_read_info.macro_block_id_ = macro_desc.macro_block_id_;
    io_read_info.mtl_tenant_id_ = MTL_ID();

    if (OB_UNLIKELY(io_read_info.size_ <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid macro read size", K(ret), K(io_read_info), K(macro_desc));
    } else if (OB_ISNULL(io_read_info.buf_ = reinterpret_cast<char *>(allocator_.alloc(io_read_info.size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc macro read info buffer", K(ret), K(io_read_info));
    } else if (OB_FAIL(ObObjectManager::async_read_object(io_read_info, macro_handle_))) {
      LOG_WARN("async read block failed", K(ret), K(io_read_info), K(macro_desc));
    } else if (OB_FAIL(macro_handle_.wait())) {
      LOG_WARN("io wait failed", K(ret), K(macro_desc), K(io_read_info));
    } else if (OB_ISNULL(macro_handle_.get_buffer())
        // 共享存储写入独立的 4KB 对齐文件，因此无法像共享无架构那样用固定 2MB 块大小校验读大小。
        || (!GCTX.is_shared_storage_mode()
            && !macro_desc.macro_block_id_.is_backup_id()
            && OB_UNLIKELY(macro_handle_.get_data_size() != io_read_info.size_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buf is null or buf size is too small", K(ret), K(macro_desc),
          KP(macro_handle_.get_buffer()), K(macro_handle_.get_data_size()), K(io_read_info.size_));
    } else if (OB_FAIL(init_from_macro_buf_(macro_handle_.get_buffer(), macro_handle_.get_data_size()))) {
      LOG_WARN("fail to parse inverted macro block after IO read", K(ret), K(macro_desc));
    } else {
      micro_block_.read_info_ = &read_info;
      datum_utils_ = &read_info.get_datum_utils();
      is_inited_ = true;
      is_opened_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObInvIndexBlockMicroIterator::init_from_macro_buf_(const char *macro_buf, const int64_t macro_size)
{
  int ret = OB_SUCCESS;
  ObMacroBlockCommonHeader common_header;
  ObSSTableMacroBlockHeader macro_header;
  int64_t pos = 0;
  if (OB_FAIL(common_header.deserialize(macro_buf, macro_size, pos))) {
    LOG_WARN("fail to deserialize common header", K(ret), K(macro_size));
  } else if (OB_FAIL(common_header.check_integrity())) {
    LOG_ERROR("invalid common header", K(ret), K(common_header));
  } else if (OB_UNLIKELY(!common_header.is_sstable_data_block())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("macro block type not supported for inv index micro iterator", K(ret), K(common_header));
  } else if (OB_FAIL(macro_header.deserialize(macro_buf, macro_size, pos))) {
    LOG_WARN("fail to deserialize macro block header", K(ret));
  } else {
    compressor_type_ = macro_header.fixed_header_.compressor_type_;
    const int64_t data_base = macro_header.fixed_header_.micro_block_data_offset_;
    const int64_t idx_off = macro_header.fixed_header_.idx_block_offset_;
    if (OB_UNLIKELY(data_base <= 0 || idx_off <= data_base || idx_off > macro_size
                    || macro_header.fixed_header_.idx_block_size_ <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inv macro data/index region layout", K(ret), K(data_base), K(idx_off),
          K(macro_size), K(macro_header));
    } else {
      data_buf_ = macro_buf + data_base;
      data_size_ = idx_off - data_base;
      const int64_t micro_block_count = macro_header.fixed_header_.micro_block_count_;
      if (micro_block_count > 0 && OB_FAIL(posting_entries_.reserve(micro_block_count))) {
        LOG_WARN("fail to reserve posting entries", K(ret), K(micro_block_count));
      } else if (micro_block_count > 0 && OB_FAIL(value_entries_.reserve(micro_block_count))) {
        LOG_WARN("fail to reserve value entries", K(ret), K(micro_block_count));
      } else if (OB_FAIL(prescan_region_(data_base))) {
        LOG_WARN("fail to prescan inverted data micro block region", K(ret), K_(data_size));
      } else if (OB_UNLIKELY(posting_entries_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no posting data micro block in macro", K(ret), K_(data_size));
      } else if (OB_FAIL(parse_index_block_(macro_buf, macro_size, macro_header, index_alloc_))) {
        LOG_WARN("fail to parse leaf term index block", K(ret), K(macro_header));
      } else if (OB_UNLIKELY(index_entries_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no term index row parsed", K(ret), K(macro_header));
      } else {
        curr_index_pos_ = -1; // 定位在第一条索引 entry 之前
      }
    }
  }
  return ret;
}

int ObInvIndexBlockMicroIterator::prescan_region_(const int64_t region_macro_base)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  while (OB_SUCC(ret) && pos < data_size_) {
    ObMicroBlockHeader header;
    const char *block_buf = data_buf_ + pos;
    const int64_t remain = data_size_ - pos;
    int64_t header_size = 0;
    int64_t stored_payload_size = 0;
    ObMicroBlockInvertedIndexType inverted_index_type = MICRO_BLOCK_INV_IDX_INVALID;
    if (OB_FAIL(header.deserialize_and_check_header(block_buf, remain))) {
      LOG_WARN("fail to deserialize inverted micro block header", K(ret), K(pos), K(remain));
    } else if (FALSE_IT(header_size = static_cast<int64_t>(header.header_size_))) {
    } else if (FALSE_IT(stored_payload_size = static_cast<int64_t>(header.data_zlength_))) {
    } else if (OB_UNLIKELY(common::INV_IDX_ROW_STORE != header.get_row_store_type())) {
      // 写入侧将倒排 payload 视为不透明净数据，统一以 INV_IDX_ROW_STORE 封帧。
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected inverted micro block row store type", K(ret), K(pos), K(header));
    } else if (FALSE_IT(inverted_index_type =
        static_cast<ObMicroBlockInvertedIndexType>(header.inverted_index_type_))) {
    } else if (OB_UNLIKELY(pos + header_size > data_size_
            || stored_payload_size <= 0
            || stored_payload_size > data_size_ - pos - header_size)) {
      // 来自磁盘的数据；以绝对偏移 pos 为基准，将 header_size 与 stored_payload_size 分别
      // 限制在 (data_size_ - pos) 内再相加，防止损坏的负数字段经加法回绕为小正数从而
      // 绕过区域检查。
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inverted micro block payload size out of bounds", K(ret), K(pos),
          K(header_size), K(stored_payload_size), K(remain), K(header));
    } else {
      const int64_t block_size = header_size + stored_payload_size;
      const char *payload_buf = nullptr;
      int64_t payload_size = 0;
      if (OB_FAIL(materialize_payload_(header, block_buf, block_size, payload_buf, payload_size))) {
        LOG_WARN("fail to materialize inverted micro block payload",
            K(ret), K(pos), K(stored_payload_size), K(header));
      } else if (MICRO_BLOCK_INV_IDX_POSTING == inverted_index_type) {
        // 宏块内绝对 header 偏移 == 索引行中的 posting_locator.offset_
        const int64_t macro_offset = region_macro_base + pos;
        if (OB_FAIL(posting_entries_.push_back(
            MicroBlockEntry(payload_buf, payload_size, macro_offset)))) {
          LOG_WARN("fail to record posting data micro block", K(ret), K(pos));
        }
      } else if (MICRO_BLOCK_INV_IDX_POSTING_VALUE == inverted_index_type) {
        // value 块携带 position_list / nested_key payload，通过聚合数据中的
        // posting_value_offset 按 term 定位（参见 parse_index_block_）。
        const int64_t macro_offset = region_macro_base + pos;
        if (OB_FAIL(value_entries_.push_back(
            MicroBlockEntry(payload_buf, payload_size, macro_offset)))) {
          LOG_WARN("fail to record posting value micro block", K(ret), K(pos));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown inverted micro block type", K(ret), K(pos), K(inverted_index_type), K(header));
      }
      if (OB_SUCC(ret)) {
        pos += block_size;
      }
    }
  }
  return ret;
}

int ObInvIndexBlockMicroIterator::materialize_payload_(
    const ObMicroBlockHeader &header,
    const char *block_buf,
    const int64_t block_size,
    const char *&payload_buf,
    int64_t &payload_size)
{
  int ret = OB_SUCCESS;
  payload_buf = nullptr;
  payload_size = 0;
  const char *stored_payload = nullptr;
  const int64_t header_size = static_cast<int64_t>(header.header_size_);
  const int64_t stored_payload_size = static_cast<int64_t>(header.data_zlength_);
  const int64_t original_payload_size = static_cast<int64_t>(header.data_length_);
  const bool is_compressed = header.is_compressed_data();
  if (OB_ISNULL(block_buf) || OB_UNLIKELY(block_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inverted micro block buffer",
        K(ret), KP(block_buf), K(block_size), K(header));
  } else if (OB_FAIL(header.check_payload_checksum(block_buf + header_size, stored_payload_size))) {
    LOG_WARN("inverted micro block payload checksum mismatch", K(ret), K(header));
  } else if (FALSE_IT(stored_payload = block_buf + header_size)) {
  } else if (!is_compressed) {
    payload_buf = stored_payload;
    payload_size = stored_payload_size;
  } else if (OB_UNLIKELY(original_payload_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid compressed inverted micro block original payload size",
        K(ret), K(header));
  } else {
    // original_payload_size (data_length_) 来自磁盘数据，不在 payload 校验和覆盖范围内，因此
    // 损坏的 header 可能声称 ~4GB 并在解压拒绝前触发巨额内存分配。64MB 远超任何合法的微块
    // payload 大小。
    const int64_t MAX_DECOMP_SIZE = 64 * 1024 * 1024; // 64MB
    if (OB_UNLIKELY(original_payload_size > MAX_DECOMP_SIZE)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inverted micro block decompressed size exceeds cap",
          K(ret), K(stored_payload_size), K(original_payload_size), K(MAX_DECOMP_SIZE));
    } else {
      // decompress_data_with_prealloc_buf 内部处理压缩器查找和大小校验；不支持的
      // compressor_type 会以 OB_NOT_SUPPORTED 暴露，无需额外防护。
      char *decomp_buf = nullptr;
      ObMacroBlockReader macro_reader;
      if (OB_ISNULL(decomp_buf = static_cast<char *>(
          allocator_.alloc(original_payload_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc decompressed inverted micro block payload", K(ret), K(header));
      } else if (OB_FAIL(macro_reader.decompress_data_with_prealloc_buf(
          compressor_type_, stored_payload, stored_payload_size,
          decomp_buf, original_payload_size))) {
        LOG_WARN("fail to decompress inverted micro block payload",
            K(ret), K(stored_payload_size), K(header));
      } else {
        payload_buf = decomp_buf;
        payload_size = original_payload_size;
      }
    }
  }
  return ret;
}

int ObInvIndexBlockMicroIterator::parse_index_block_(
    const char *macro_buf,
    const int64_t macro_size,
    const ObSSTableMacroBlockHeader &macro_header,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t idx_off = macro_header.fixed_header_.idx_block_offset_;
  // rowkey/column 数量取自叶子索引微块 header（rowkey endkey + 1 个聚合列），而非
  // 宏块级别的 data-row column_count。在下方解压之后填充。
  int64_t rowkey_count = 0;
  int64_t column_count = 0;
  const char *micro_buf = macro_buf + idx_off;
  int64_t pos = 0;
  bool is_compressed = false;
  ObMicroBlockHeader micro_header;
  ObMacroBlockReader macro_reader;
  ObMicroBlockData index_block;
  ObMicroBlockReaderHelper reader_helper;
  ObIMicroBlockReader *reader = nullptr;
  ObDatumRow index_row;
  if (OB_FAIL(micro_header.deserialize(micro_buf, macro_size - idx_off, pos))) {
    LOG_WARN("fail to deserialize leaf index micro header", K(ret), K(idx_off), K(macro_size));
  } else {
    const int64_t idx_buf_size = macro_size - idx_off;
    // 来自磁盘的数据；先将 header_size 和 data_zlength_ 分别限制在 idx_buf_size 内再
    // 做加法，否则负数 data_zlength_ 可能导致加法回绕为一个小的正数从而绕过区域检查。
    const int64_t header_size = micro_header.header_size_;
    const int64_t data_zlength = micro_header.data_zlength_;
    int64_t micro_buf_size = 0;
    if (OB_UNLIKELY(header_size <= 0 || header_size > idx_buf_size
            || data_zlength < 0 || data_zlength > idx_buf_size - header_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("leaf index micro block size out of bounds", K(ret),
          K(header_size), K(data_zlength), K(idx_buf_size), K(micro_header));
    } else if (FALSE_IT(micro_buf_size = header_size + data_zlength)) {
    } else if (OB_UNLIKELY(micro_buf_size > idx_buf_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("leaf index micro block size out of bounds", K(ret),
          K(micro_buf_size), K(idx_buf_size), K(micro_header));
    } else if (OB_FAIL(micro_header.check_record(micro_buf, micro_buf_size, MICRO_BLOCK_HEADER_MAGIC))) {
      LOG_WARN("fail to check leaf index micro header", K(ret), K(micro_header));
    } else if (OB_FAIL(macro_reader.decrypt_and_decompress_data(
        macro_header, micro_buf, micro_buf_size, false /*need_deep_copy*/, index_block, is_compressed))) {
      LOG_WARN("fail to decompress leaf index micro block", K(ret), K(micro_buf_size));
    } else if (OB_FAIL(reader_helper.init(allocator))) {
      LOG_WARN("fail to init micro reader helper", K(ret));
    } else if (OB_FAIL(reader_helper.get_reader(*index_block.get_micro_header(), reader))) {
      LOG_WARN("fail to get index micro reader", K(ret));
    } else if (OB_FAIL(reader->init(index_block, nullptr /*datum_utils*/))) {
      LOG_WARN("fail to init index micro reader", K(ret));
    } else if (FALSE_IT(rowkey_count = index_block.get_micro_header()->rowkey_column_count_)) {
    } else if (FALSE_IT(column_count = index_block.get_micro_header()->column_count_)) {
    } else if (OB_UNLIKELY(rowkey_count <= 0 || column_count != rowkey_count + 1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid inverted index rowkey/column count", K(ret), K(rowkey_count), K(column_count));
    } else if (OB_FAIL(index_row.init(allocator, column_count))) {
      LOG_WARN("fail to init index row", K(ret), K(column_count));
    } else {
      const int64_t row_count = index_block.get_micro_header()->row_count_;
      if (OB_FAIL(index_entries_.reserve(row_count))) {
        LOG_WARN("fail to reserve index entries", K(ret), K(row_count));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
        index_row.reuse();
        ObIndexBlockRowParser parser;
        const ObIndexBlockRowHeader *row_header = nullptr;
        const char *agg_buf = nullptr;
        int64_t agg_size = 0;
        int64_t entry_idx = -1;
        IndexEntry *entry = nullptr;
        ObDatumRowkey raw_endkey;
        if (OB_FAIL(reader->get_row(i, index_row))) {
          LOG_WARN("fail to read index row", K(ret), K(i), K(row_count));
        } else if (OB_FAIL(parser.init(rowkey_count, index_row))) {
          LOG_WARN("fail to init index row parser", K(ret), K(i), K(rowkey_count));
        } else if (OB_FAIL(parser.get_header(row_header))) {
          LOG_WARN("fail to get index row header", K(ret), K(i));
        } else if (OB_UNLIKELY(!row_header->is_inverted_index())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index row is not an inverted index agg row", K(ret), K(i), KPC(row_header));
        } else if (OB_FAIL(parser.get_agg_row(agg_buf, agg_size))) {
          LOG_WARN("fail to get inverted index agg row", K(ret), K(i));
        } else if (OB_FAIL(find_entry_idx_(posting_entries_, row_header->get_block_offset(), entry_idx))) {
          LOG_WARN("fail to map index row to posting block", K(ret), K(i),
              "block_offset", row_header->get_block_offset());
        } else if (OB_FAIL(raw_endkey.assign(index_row.storage_datums_, rowkey_count))) {
          LOG_WARN("fail to assign raw endkey", K(ret), K(i), K(rowkey_count));
        } else if (OB_ISNULL(entry = index_entries_.alloc_place_holder())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to grow index entry array", K(ret), K(i));
        } else {
          // 原地填充：index_entries_ 已预留 row_count 容量，指针保持有效，
          // 也避免按值插入时拷贝 IndexEntry 中较大的内联 metas_ 缓冲。
          int64_t posting_value_offset = 0;
          int64_t posting_value_size = 0;
          entry->posting_entry_idx_ = entry_idx;
          entry->value_entry_idx_ = -1;
          if (OB_FAIL(raw_endkey.deep_copy(entry->endkey_, index_alloc_))) {
            LOG_WARN("fail to deep copy endkey", K(ret), K(i));
          } else if (OB_FAIL(ObInvIdxAggDataDecoder::decode(agg_buf, agg_size, entry->metas_,
              posting_value_offset, posting_value_size))) {
            LOG_WARN("fail to decode inverted index agg data", K(ret), K(i), K(agg_size));
          } else if (posting_value_size > 0) {
            // 支持短语查询的 sstable：通过 posting_value_offset 绑定配对的 value 块。
            if (OB_FAIL(find_entry_idx_(value_entries_, posting_value_offset, entry->value_entry_idx_))) {
              LOG_WARN("fail to map index row to value block", K(ret), K(i),
                  K(posting_value_offset), K(posting_value_size));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObInvIndexBlockMicroIterator::find_entry_idx_(
    const common::ObIArray<MicroBlockEntry> &entries,
    const int64_t macro_offset, int64_t &entry_idx) const
{
  int ret = OB_SUCCESS;
  entry_idx = -1;
  // entries 按 macro_offset_ 升序排列（prescan 中 pos 严格递增），因此二分查找有效，
  // 避免了每条索引行 O(n) 的线性探测，总复杂度从 O(n^2) 降为 O(n log n)。
  const int64_t count = entries.count();
  if (count > 0) {
    const MicroBlockEntry *begin = &entries.at(0);
    const MicroBlockEntry *end = begin + count;
    const MicroBlockEntry *it = std::lower_bound(begin, end, macro_offset, MicroBlockOffsetLess());
    if (it != end && it->macro_offset_ == macro_offset) {
      entry_idx = it - begin;
    }
  }
  if (OB_UNLIKELY(-1 == entry_idx)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("framed micro block offset not found in data region", K(ret),
        K(macro_offset), K(entries));
  }
  return ret;
}

int ObInvIndexBlockMicroIterator::fill_micro_block_from_index_(ObMicroBlock &mb)
{
  int ret = OB_SUCCESS;
  const IndexEntry &entry = index_entries_.at(curr_index_pos_);
  if (OB_UNLIKELY(entry.posting_entry_idx_ < 0
      || entry.posting_entry_idx_ >= posting_entries_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index entry points to invalid posting block", K(ret), K_(curr_index_pos),
        K(entry.posting_entry_idx_), "posting_count", posting_entries_.count());
  } else {
    const MicroBlockEntry &posting = posting_entries_.at(entry.posting_entry_idx_);
    // 倒排数据微块磁盘上有 ObMicroBlockHeader 帧，已在 materialize 时剥离并解压， 此处的 payload 就是净数据。
    mb.payload_data_.get_buf() = posting.payload_buf_;
    mb.payload_data_.get_buf_size() = posting.payload_size_;
    mb.data_.get_buf() = posting.payload_buf_;
    mb.data_.get_buf_size() = posting.payload_size_;
    mb.data_.micro_header_ = nullptr;
    mb.data_.type_ = ObMicroBlockData::INV_IDX_POSTING_BLOCK;
    // range_：对 term endkey 左开右闭（对齐 compaction 版本）
    if (0 == curr_index_pos_) {
      mb.range_.start_key_ = range_.get_start_key();
    } else {
      mb.range_.start_key_ = index_entries_.at(curr_index_pos_ - 1).endkey_;
    }
    mb.range_.end_key_ = entry.endkey_;
    mb.range_.border_flag_.unset_inclusive_start();
    mb.range_.border_flag_.set_inclusive_end();
    // header_ 保持默认值（无标准 micro header）；micro_index_info_ 保持 nullptr。
    mb.read_info_ = read_info_;
  }
  return ret;
}

int ObInvIndexBlockMicroIterator::get_next_term_entry(
    const ObMicroBlock *&micro_block,
    const common::ObIArray<ObInvIdxLogicalBlockMeta> *&logical_block_metas)
{
  int ret = OB_SUCCESS;
  micro_block = nullptr;
  logical_block_metas = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (curr_index_pos_ + 1 >= index_entries_.count()) {
    curr_index_pos_ = index_entries_.count(); // 停留在末尾
    ret = OB_ITER_END;
  } else {
    ++curr_index_pos_;
    if (OB_FAIL(fill_micro_block_from_index_(micro_block_))) {
      LOG_WARN("fail to fill micro block from index entry", K(ret), K_(curr_index_pos));
    } else {
      micro_block = &micro_block_;
      // 本 term 的逻辑块 meta，按 block_idx 与 payload 块一一对应。
      // 借用；在下一次 get_next_term_entry()/reset() 前有效。
      logical_block_metas = &index_entries_.at(curr_index_pos_).metas_;
    }
  }
  return ret;
}

int ObInvIndexBlockMicroIterator::get_curr_value_micro_block(ObMicroBlockData &value_block) const
{
  int ret = OB_SUCCESS;
  value_block.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(curr_index_pos_ < 0 || curr_index_pos_ >= index_entries_.count())) {
    ret = OB_ITER_END;
  } else {
    const IndexEntry &entry = index_entries_.at(curr_index_pos_);
    if (entry.value_entry_idx_ >= 0 && entry.value_entry_idx_ < value_entries_.count()) {
      const MicroBlockEntry &value_entry = value_entries_.at(entry.value_entry_idx_);
      value_block.get_buf() = value_entry.payload_buf_;
      value_block.get_buf_size() = value_entry.payload_size_;
      value_block.type_ = ObMicroBlockData::INV_IDX_POSTING_BLOCK;
    }
    // 否则：该 sstable 无 posting value —— 保持 reset 状态（调用方跳过短语列）。
  }
  return ret;
}

void ObInvIndexBlockMicroIterator::reset()
{
  is_inited_ = false;
  is_opened_ = false;
  macro_handle_.reset();
  allocator_.reset();
  posting_entries_.reset();
  value_entries_.reset();
  index_entries_.reset();
  index_alloc_.reset();
  datum_utils_ = nullptr;
  read_info_ = nullptr;
  data_buf_ = nullptr;
  data_size_ = 0;
  curr_index_pos_ = -1;
  range_.reset();
  micro_block_ = ObMicroBlock();
  compressor_type_ = common::INVALID_COMPRESSOR;
}

} // end namespace blocksstable
} // end namespace oceanbase

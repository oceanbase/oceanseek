/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_ITERATOR_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_ITERATOR_H_

#include "lib/allocator/page_arena.h"
#include "lib/compress/ob_compressor_pool.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_iarray.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "share/io/ob_io_manager.h"
#include "share/schema/ob_table_schema.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/blocksstable/async_index/ob_inv_idx_agg_data_encoder.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_writer.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/blocksstable/ob_imicro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_header.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_sstable_macro_block_header.h"

namespace oceanbase
{
namespace blocksstable
{

class ObInvSSTable;
class ObStorageDatumUtils;

// ObInvIndexBlockMicroIterator —— 倒排索引读栈的顶层（MACRO -> MICRO）。
// 对外接口对齐 compaction::ObIndexBlockMicroIterator。
//
// 倒排宏块内只包含单个 term-index 叶子块（ObInvIdxAggDataEncoder 格式），而非标准的
// 多级索引树，因此 ObMicroIndexInfo / endkey 不存在。get_next_term_entry() 产出的
// ObMicroBlock 中 payload_data_ 指向已剥帧的 posting payload，但 ObMicroBlockHeader 字段
// 保持默认值、micro_index_info_ 为空，因此 ObMicroBlock::is_valid() 返回 false —— 调用方
// 必须直接使用 payload_data_，且绝对不能调用 is_valid()。
//
// 迭代域为 term-index 行：每步产出一个 (posting payload M, term T) 对以及 T 的逻辑块
// meta。每个 (M, T) 产生一条 entry —— 一个包含多个 term 的微块每个 term 产出一次；
// 一个 term 跨多个微块则每个微块产出一次 —— 因此重建某个 term 完整 posting list 的
// 调用方需要拼接连续的同 term entry。
//
// 短语查询支持：数据区域交错存储 posting-data 和 posting-value 微块。数据块存入
// posting_entries_，value 块存入 value_entries_；每条索引行的聚合数据携带 M 所配对
// value 块的宏块内绝对偏移，通过 get_curr_value_micro_block() 暴露。
//
// 生命周期：宏块缓冲区、解压后的 payload 和解析出的 entry 由内部分配器持有，有效性
// 持续到 reset()。返回的 ObMicroBlock / metas 视图为借用，在下一次
// get_next_term_entry()/reuse()/reset() 后失效。
class ObInvIndexBlockMicroIterator final
{
public:
  ObInvIndexBlockMicroIterator();
  ~ObInvIndexBlockMicroIterator();

  // 通过异步 IO 读取宏块，然后反序列化 common/macro header，预扫描数据区域并解析
  // term-index 叶子块。
  int init(const ObMacroBlockDesc &macro_desc,
           const ObITableReadInfo &read_info,
           const ObInvSSTable *sstable);

  void reset();

  // 前进到下一条 term-index entry：用该 entry 的 posting payload 填充 micro_block，
  // 用该 term 的逻辑块 meta 填充 logical_block_metas。每个 (micro block M, term T)
  // 产出一条 entry —— 粒度约定详见类注释。越界后返回 OB_ITER_END，init() 调用前
  // 返回 OB_NOT_INIT；非成功返回时两个输出参数均为空。两个输出均为借用，在下一次
  // get_next_term_entry()/reuse()/reset() 后失效。
  int get_next_term_entry(const ObMicroBlock *&micro_block,
                          const common::ObIArray<ObInvIdxLogicalBlockMeta> *&logical_block_metas);

  // 与当前 term entry 的 posting-data 块配对的 value (position) 微块 payload。
  // 当该 sstable 无 posting value（无短语列）时保持 reset 状态。借用；在一次成功
  // 的 get_next_term_entry() 后有效，在下一次 get_next_term_entry()/reuse()/reset()
  // 后失效。
  int get_curr_value_micro_block(ObMicroBlockData &value_block) const;

  // 当前产出的 entry 是否为第一条 / 最后一条 term-index 行。
  OB_INLINE bool is_left_border() const { return is_opened_ && 0 == curr_index_pos_; }
  OB_INLINE bool is_right_border() const
  {
    return is_opened_ && curr_index_pos_ >= 0
        && curr_index_pos_ == index_entries_.count() - 1;
  }

  TO_STRING_KV(K_(is_opened), "index_entry_count", index_entries_.count(),
      K_(curr_index_pos));

private:
  // 在 open() 预扫描阶段定位到的一个带帧的倒排数据微块（posting data 或 posting value）。
  struct MicroBlockEntry
  {
    MicroBlockEntry() : payload_buf_(nullptr), payload_size_(0), macro_offset_(-1) {}
    MicroBlockEntry(const char *buf, const int64_t size, const int64_t macro_offset)
      : payload_buf_(buf), payload_size_(size), macro_offset_(macro_offset) {}
    TO_STRING_KV(KP_(payload_buf), K_(payload_size), K_(macro_offset));
    const char *payload_buf_;  // payload 起始位置（物理 header 之后），借用指针
    int64_t payload_size_;
    int64_t macro_offset_;     // 此块的物理 header 在宏块内的绝对偏移
  };

  // 按 macro_offset_ 比较 MicroBlockEntry；供 find_entry_idx_ 的 std::lower_bound 使用。
  struct MicroBlockOffsetLess
  {
    bool operator()(const MicroBlockEntry &entry, const int64_t macro_offset) const
    {
      return entry.macro_offset_ < macro_offset;
    }
  };

  // 一条解析后的 term-index 行。宏块中通常有很多这样的行，且每行至少包含
  // 一个 meta；若 metas_ 使用默认 block size 的 ObArray，每行首次插入都会产生一次
  // 堆分配，放大总内存和碎片。因此与写入侧 logical_block_metas_ 保持一致，
  // 用 ObSEArray 内联 4 个 meta；超出内联容量后使用当前租户的 allocator。
  struct IndexEntry
  {
    IndexEntry()
      : endkey_(),
        posting_entry_idx_(-1),
        value_entry_idx_(-1),
        metas_(OB_MALLOC_NORMAL_BLOCK_SIZE,
            common::ModulePageAllocator(lib::ObLabel("InvIdxMicroIt"), MTL_ID()))
    {}
    TO_STRING_KV(K_(endkey), K_(posting_entry_idx), K_(value_entry_idx), K_(metas));
    ObDatumRowkey endkey_;            // (term..., max_doc_id)，不含多版本列
    int64_t posting_entry_idx_;       // 本行 posting 块在 posting_entries_ 中的下标
    int64_t value_entry_idx_;         // M 所配对 value 块在 value_entries_ 中的下标，无则为 -1
    common::ObSEArray<ObInvIdxLogicalBlockMeta, 4> metas_;  // 本 term 的逻辑块 meta
  };

  // 遍历数据区域中每个带帧的微块，校验其帧格式，记录 posting/value 块及其宏块内绝对偏移。
  // region_macro_base 为 data_buf_ 在宏块内的绝对偏移。
  int prescan_region_(const int64_t region_macro_base);
  // 返回 header 所描述的解压后的 payload。未压缩 payload 直接借用 block_buf 内部指针；
  // 压缩 payload 解压到 allocator_ 中。
  int materialize_payload_(const ObMicroBlockHeader &header,
                           const char *block_buf,
                           const int64_t block_size,
                           const char *&payload_buf,
                           int64_t &payload_size);
  // 将宏块内的叶子 term-index 微块解析为有序的 index_entries_。
  int parse_index_block_(const char *macro_buf,
                         const int64_t macro_size,
                         const ObSSTableMacroBlockHeader &macro_header,
                         common::ObIAllocator &allocator);
  // 将带帧微块的宏块内绝对 header 偏移映射为其在 entries（posting_entries_ 或
  // value_entries_）中的下标。
  int find_entry_idx_(const common::ObIArray<MicroBlockEntry> &entries,
                      const int64_t macro_offset, int64_t &entry_idx) const;
  // 用当前游标 entry 的 posting payload + term 范围填充 micro_block_。
  int fill_micro_block_from_index_(ObMicroBlock &mb);
  // 反序列化 IO 读取的宏块缓冲区的 common/macro header，然后预扫描数据区域并解析
  // term-index 叶子块。
  int init_from_macro_buf_(const char *macro_buf, const int64_t macro_size);

private:
  const char *data_buf_;   // 数据区域起始位置（在被持有的宏块缓冲区内），借用指针
  int64_t data_size_;
  // 宏块内 posting/value/term-index 数量通常较多，用 ObArray 避免 SEArray 本地缓冲白占体积。
  common::ObArray<MicroBlockEntry> posting_entries_; // posting data 块，按写入顺序
  common::ObArray<MicroBlockEntry> value_entries_;   // posting value 块，按写入顺序

  common::ObArenaAllocator index_alloc_;               // 持有解析后的 endkey
  common::ObArray<IndexEntry> index_entries_;          // term-index 行，endkey 升序
  const ObStorageDatumUtils *datum_utils_;             // 用于 term entry 的范围物化
  int64_t curr_index_pos_;                             // index_entries_ 游标，首个之前为 -1
  bool is_opened_;

  ObStorageObjectHandle macro_handle_;                 // 宏块的异步读 handle
  common::ObArenaAllocator allocator_;                 // 持有 IO 读取的宏块缓冲区
  ObMicroBlock micro_block_;                           // 由 get_next_term_entry() 填充的输出对象
  const ObITableReadInfo *read_info_;                  // 用于 micro_block_.read_info_ + datum_utils
  ObDatumRange range_;                                 // 宏块范围，用于 micro_block_.range_
  common::ObCompressorType compressor_type_;           // 宏块级压缩器，解压微块 payload 时使用
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObInvIndexBlockMicroIterator);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_MICRO_BLOCK_ITERATOR_H_

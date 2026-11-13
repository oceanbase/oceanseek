/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_LOB_ITERATOR_H_
#define OCEANBASE_STORAGE_OB_LOB_ITERATOR_H_

#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/page_arena.h"
#include "lib/restore/ob_storage_info.h"
#include "lib/string/ob_string.h"
#include "lib/charset/ob_charset.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/store/ob_lob_meta.h"
#include "storage/lob/store/ob_lob_aux_manager.h"
#include "storage/lob/access/ob_lob_remote.h"

namespace oceanbase
{
namespace common
{
class ObObjectStorageInfo;
}

namespace storage
{

// @brief Lob 查询迭代器的基类，支持 inrow 和 outrow，支持两种返回格式
// 1. 返回 ObString，调用者需提前申请内存，迭代器会尽可能填满，直到迭代完整个 lob
// 2. 返回 ObLobChunk，调用者无需申请内存，如果 lob 是 inrow，则返回包含 inplace 的 chunk 结构体，
//    若 lob 是 outrow，则返回调用者指定的 chunk_size 大小的分片，同一个迭代器多次调用时 chunk_size 必须一致
class ObLobQueryIter
{
public:
  ObLobQueryIter()
    : cs_type_(CS_TYPE_BINARY), is_reverse_(false), is_end_(false), is_inited_(false)
  {}

  virtual ~ObLobQueryIter() { }

  // @brief get_next_row 获取下一批次的数据，data 内存需要调用者申请好
  virtual int get_next_row(ObString &data) = 0;
  virtual int get_next_chunk(ObLobChunk &chunk, const int64_t chunk_size);
  virtual void reset() = 0;
  bool is_end() const { return is_end_; }

protected:
  // @brief 校准覆盖 data 的 byte length，data 需包含完整的字符边界，
  // 调整后的 byte_length 不会超出 data 边界，并且包含完整的字符。若 [0, n - 1] 表示 data 区间,
  // is_reverse 为 false 时，[0, aligned_byte_length] 为校准后的区间,
  // is_reverse 为 true 时，[n - aligned_byte_length, n - 1] 为校准后的区间。
  // @param data[in]
  // @param is_reverse[in], 是否是倒序查询
  // @param cs_type[in], data 数据的字符集
  // @parma byte_length[in], 调整前的 byte length
  // @param aligned_byte_length[out]，调整后的 byte length
  static int align_length_(const ObString &data,
                           const bool is_reverse,
                           const ObCollationType cs_type,
                           const int64_t byte_length,
                           int64_t &aligned_byte_length);
  // Compute both byte and character lengths while splitting chunks sequentially to avoid rescanning.
  static int align_forward_chunk_(const ObString &data,
                                  const ObCollationType cs_type,
                                  const int64_t byte_length,
                                  int64_t &aligned_byte_length,
                                  int64_t &aligned_char_length);
  // @brief 使用 data 填充 buffer，data 若有剩余，则保存在 remain_data 里面
  // @param buffer[out]，需要提前分配好内存，拷贝的输出
  // @param data[in]，输入数据
  // @param remain_data，未拷贝完成的数据
  int fill_buffer_(ObString &buffer,
                   const ObString &data,
                   ObString &remain_data);
  virtual int get_next_chunk_impl_(ObLobChunk &chunk, const int64_t chunk_size) = 0;

protected:
  static constexpr int64_t OB_MIN_CHUNK_SIZE = 1024; // 1K
  static constexpr int64_t OB_MAX_CHUNK_SIZE = 2L * 1024 * 1024; // 2M
  ObCollationType cs_type_;
  bool is_reverse_;
  bool is_end_;
  bool is_inited_;
};

// @brief Inrow Lob 的 Query Iter，是 inrow 数据以迭代器方式遍历的抽象实现
class ObLobInRowQueryIter : public ObLobQueryIter
{
public:
  ObLobInRowQueryIter() : ObLobQueryIter(), orgin_data_(), remain_data_(0) {}

  virtual ~ObLobInRowQueryIter() { reset(); }

  int open(ObString &data, uint32_t byte_offset, uint32_t byte_len, ObCollationType cs, bool is_reverse = false); // inrow open
  virtual int get_next_row(ObString &data);
  virtual void reset();

  TO_STRING_KV(
    K_(cs_type),
    K_(is_reverse),
    K_(is_end),
    K_(is_inited),
    "orgin_data_length", orgin_data_.length(),
    "remain_data_length", remain_data_.length(),
    K_(orgin_data),
    K_(remain_data)
  );

private:
  virtual int get_next_chunk_impl_(ObLobChunk &chunk, const int64_t chunk_size) final;

private:
  ObString orgin_data_;
  ObString remain_data_;

  DISALLOW_COPY_AND_ASSIGN(ObLobInRowQueryIter);
};

// @brief Out-row Lob 的 Query Iter，是 out-row 数据以迭代器方式遍历的抽象实现，支持远程读取
//        - 本地读取由 ObLobChunkScanIter 实现，远程读取由 ObLobRemoteQueryHandler 实现
//        - get_next_row 将块/分块数据填充到调用者的缓冲区中，因此一个分块完全消耗后，才会发出下一个 get_next_chunk 请求
class ObLobOutRowQueryIter : public ObLobQueryIter
{
public:
  ObLobOutRowQueryIter()
    : ObLobQueryIter(), is_remote_(false), allocator_(), chunk_iter_(), remote_handler_(),
      read_ctx_(), last_data_(), buffer_()
  {
    ObMemAttr attr(MTL_ID(), "LobOutRowIter");
    allocator_.set_attr(attr);
  }
  virtual ~ObLobOutRowQueryIter() { reset(); }

  int open(const ObLobReadContext &read_ctx, ObLobMetaManager *lob_meta_mngr);
  virtual int get_next_row(ObString &data);
  virtual void reset();

private:
  int get_next_block(ObString &block_data);
  virtual int get_next_chunk_impl_(ObLobChunk &chunk, const int64_t chunk_size) final;

private:
  bool is_remote_;
  ObArenaAllocator allocator_;
  ObLobChunkScanIter chunk_iter_;
  ObLobRemoteQueryHandler remote_handler_;
  ObLobReadContext read_ctx_;
  ObString last_data_;
  ObString buffer_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobOutRowQueryIter);
};

// @brief LOB 追加数据源迭代器，按顺序将两个内存缓冲区作为连续数据源输出
class ObLobAppendSourceIter final : public ObLobQueryIter
{
public:
  ObLobAppendSourceIter()
    : ObLobQueryIter(), allocator_(nullptr), first_buffer_(), second_buffer_(),
      first_buffer_pos_(0), second_buffer_pos_(0), buffer_()
  {}
  virtual ~ObLobAppendSourceIter() { reset(); }

  int open(common::ObIAllocator *allocator,
           const ObString &first_buffer,
           const ObString &second_buffer,
           const ObCollationType cs_type);
  virtual int get_next_row(ObString &buffer) override;
  virtual void reset() override;

private:
  virtual int get_next_chunk_impl_(ObLobChunk &chunk, const int64_t chunk_size) final;

private:
  common::ObIAllocator *allocator_;
  // The iterator consumes two buffers in order. first_buffer_ stores old in-row data or a partially filled
  // tail chunk, while second_buffer_ stores the appended data.
  ObString first_buffer_;
  ObString second_buffer_;
  int64_t first_buffer_pos_;
  int64_t second_buffer_pos_;
  // 保存 get_next_chunk() 返回的可复用输出缓冲区。
  ObString buffer_;

  DISALLOW_COPY_AND_ASSIGN(ObLobAppendSourceIter);
};

// Compute pieces and IDs in the declared remote range; do not allocate or materialize macroblocks.
class ObExternalLobMacroIterator
{
public:
  static constexpr uint64_t PIECE_SIZE = 2UL * 1024 * 1024;
  static constexpr uint64_t MAX_PIECE_COUNT = 64UL * 1024;

  ObExternalLobMacroIterator() { reset(); }
  int init(uint64_t lob_meta_tablet_id,
      uint64_t external_lob_macro_seq_id, uint64_t offset, uint64_t length);
  int get_next(blocksstable::MacroBlockId &macro_id);
  void reset();

private:
  uint64_t lob_meta_tablet_id_;
  uint64_t external_lob_macro_seq_id_;
  uint64_t next_offset_;
  uint64_t end_offset_;
  bool is_inited_;
};

// External and Internal outrow use separate readers; results are always raw binary data.
class ObExternalLobQueryIter final : public ObLobQueryIter
{
public:
  ObExternalLobQueryIter();
  virtual ~ObExternalLobQueryIter() { reset(); }
  int open(const ObLobReadContext &read_ctx,
           const common::ObExternalBlobInfo &external_info,
           uint64_t lob_meta_tablet_id,
           uint64_t external_lob_macro_seq_id,
           const common::ObObjectStorageInfo &access_info);
  virtual int get_next_row(common::ObString &buffer) override;
  virtual void reset() override;

private:
  int get_next_block(ObString &block_data);
  int check_etag_still_match();
  virtual int get_next_chunk_impl_(ObLobChunk &chunk, int64_t chunk_size) override;

private:
  ObArenaAllocator allocator_;
  common::ObExternalBlobInfo external_info_;
  ObExternalLobMacroIterator piece_iter_;
  common::ObObjectStorageInfo access_info_;
  blocksstable::ObStorageObjectHandle io_handle_;
  uint64_t tenant_id_;
  uint64_t read_start_;
  uint64_t read_end_;
  uint64_t read_pos_;
  int64_t timeout_us_;
  char *io_buffer_;
  common::ObString remaining_data_;
  common::ObString buffer_;
  bool reached_eof_;

  DISALLOW_COPY_AND_ASSIGN(ObExternalLobQueryIter);
};

}  // storage
}  // oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_ITERATOR_H_

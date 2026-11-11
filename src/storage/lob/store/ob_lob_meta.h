/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEABASE_STORAGE_OB_LOB_META_
#define OCEABASE_STORAGE_OB_LOB_META_
#include "lib/charset/ob_charset.h"
#include "lib/container/ob_se_array.h"
#include "ob_lob_store_common.h"
#include "lib/task/ob_timer.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/access/ob_dml_param.h"
#include "storage/lob/common/ob_lob_locator.h"
#include "storage/lob/common/ob_lob_seq.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/store/ob_lob_persistent_adaptor.h"
#include "storage/lob/store/ob_lob_outtable_io.h"
#include "storage/lob/access/ob_lob_remote.h"

namespace oceanbase
{
namespace storage
{

class ObPersistentLobAdaptor;
struct ObLobMetaScanResult;
class ObLobMetaScanIter;

struct ObLobMetaScanResult 
{
  ObLobMetaScanResult() : info_(), st_(0), len_(0) {}
  ObLobMetaInfo info_;
  // TODO rename start_pos_ and length_
  uint64_t st_;
  uint64_t len_;
  TO_STRING_KV(K_(info), K_(st), K_(len));
};

// @brief 
// Lob Meta 表的 Scan Iter，提供范围查询能力，返回指定范围内相关的 meta 信息（ObLobMetaInfo)。
// 需注意第一个和最后一个 ObLobMetaInfo 可能存在部分范围不在查询范围内，因此 Iter 每次返回的
// ObLobMetaScanResult 中携带了 start 和 length。
// 
// 用户提供的 param 需指定 lob 的 charset 类型，ObLobMetaScanIter 会自适应，当 lob 是字符语义时
// 会按照字符语义进行范围查询
class ObLobMetaScanIter
{
public:
  ObLobMetaScanIter();
  ~ObLobMetaScanIter() { reset(); }
  void reset();
  int open(ObLobReadContext &read_ctx, ObPersistentLobAdaptor *adaptor);
  // Reuse the underlying table scan with the range specified by read_ctx.
  int rescan(ObLobReadContext &read_ctx);
  int get_next_row(ObLobMetaScanResult &result);

  // the memory of cur_info may be released by storage, so do not print cur_info directly
  TO_STRING_KV(K_(is_opened), K_(coll_type), K_(offset), K_(len), K_(scan_backward),
               K_(cur_byte_pos), K_(cur_char_pos), K(cur_info_.lob_id_));
private:
  void update_cur_pos_();
  bool is_char() const { return coll_type_ != common::ObCollationType::CS_TYPE_BINARY; }
  // @brief true if info intersects query range [offset_, offset_ + len_)
  bool is_in_range(const ObLobMetaInfo &info);
  // @brief true if the scan position has passed the query range end
  bool is_range_over();
private:
  bool is_opened_;
  ObPersistentLobAdaptor *adaptor_;
  ObLobMetaTableIter *meta_iter_;
  ObCollationType coll_type_;
  uint64_t offset_;
  uint64_t len_;
  bool scan_backward_;
  uint64_t cur_pos_;
  uint64_t cur_byte_pos_;
  uint64_t cur_char_pos_;
  ObLobMetaInfo cur_info_;
  // backing buffer for param.seq_id_start_ (seq idx 1), referenced by the scan range
  // during the whole meta scan, so it must live as long as meta_iter_.
  char seq_id_start_buf_[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
};

// @brief Local streaming chunk assembler.
// Scans the requested meta range first to get the start and last seq_ids, then
// rescans meta from the start seq_id. The bounded piece scan pairs meta and
// piece rows in lockstep (both ordered by seq_id, 1:1) while
// retaining only O(1) meta state. Each get_next_chunk slices the piece data by
// the meta result's [st_, len_) and returns one ObLobChunk (zero-copy into the piece
// scan buffer, valid until the next get_next_chunk).
class ObLobChunkScanIter
{
public:
  ObLobChunkScanIter();
  ~ObLobChunkScanIter();
  void reset();
  int open(ObLobReadContext &read_ctx, ObPersistentLobAdaptor *adaptor);
  int get_next_chunk(ObLobChunk &chunk);

  TO_STRING_KV(K_(is_opened), K_(piece_opened), K_(is_char), K_(scan_backward), K_(coll_type), KPC(read_ctx_));
private:
  // Prepare and open the bounded piece scan.
  int prepare_piece_iter_();
  // Open the piece scan with two seq_id range endpoints
  int open_piece_iter_(const ObString &lower_seq_id, const ObString &upper_seq_id);
  int read_in_table_data(const ObLobMetaScanResult &meta_result, ObString &data);
  int read_outtable_data(const ObLobMetaScanResult &meta_result, ObString &data);
  // Slice the current chunk using meta_result.st_ and meta_result.len_.
  int fill_chunk_(const ObLobMetaScanResult &meta_result, const ObString &data, ObLobChunk &chunk);
private:
  bool is_opened_;
  bool piece_opened_;
  bool is_char_;
  bool scan_backward_;
  ObCollationType coll_type_;
  ObPersistentLobAdaptor *adaptor_;
  ObArenaAllocator allocator_;
  ObLobOutTableChunkReader outtable_reader_;
  ObLobReadContext *read_ctx_;
  ObLobMetaScanIter meta_iter_;
  ObLobPieceTableIter *piece_iter_;
};

class ObLobQueryIter;
class ObLobMetaManager;
class ObLobManager;

enum class ObLobWriteResultType : uint8_t
{
  INVALID = 0,
  PIECE = 1,
  META_SUMMARY = 2,
  META_CHUNK = 3,
  MAX
};

struct ObLobWriteResult
{
  ObLobWriteResult() : type_(ObLobWriteResultType::INVALID), meta_info_(), piece_info_() {}
  void reset()
  {
    type_ = ObLobWriteResultType::INVALID;
    meta_info_.reset();
    piece_info_.reset();
  }
  bool is_piece() const { return ObLobWriteResultType::PIECE == type_; }
  bool is_meta() const
  {
    return ObLobWriteResultType::META_SUMMARY == type_
        || ObLobWriteResultType::META_CHUNK == type_;
  }
  bool is_meta_summary() const { return ObLobWriteResultType::META_SUMMARY == type_; }
  bool is_meta_chunk() const { return ObLobWriteResultType::META_CHUNK == type_; }
  TO_STRING_KV(K_(type), K_(meta_info), K_(piece_info));

  ObLobWriteResultType type_;
  // meta 字符串由 result_allocator_ 持有，当前 LOB drain 完成前有效。
  ObLobMetaInfo meta_info_;
  // piece 的 seq_id 和 payload 仅在下一次 get_next_row() 或 reuse() 前有效。
  ObLobPieceInfo piece_info_;
};

// 描述当前待读取的源 LOB。read_ctx_ 必须绑定当前 locator 和读取范围；持久化
// out-row LOB 还需要绑定 location 和 snapshot。read_ctx_、read_ctx_->allocator_ 和
// lob_manager_ 均由调用方持有，生命周期必须覆盖当前 LOB 的完整 drain。
struct ObLobSourceReadCtx
{
  ObLobSourceReadCtx()
    : read_ctx_(nullptr), lob_manager_(nullptr), source_byte_len_(UINT64_MAX)
  {}
  bool is_valid() const
  {
    return nullptr != read_ctx_
        && nullptr != read_ctx_->allocator_
        && !read_ctx_->src_loc_.is_null()
        && read_ctx_->src_loc_.is_valid(false)
        && ObCharset::is_valid_collation(read_ctx_->coll_type_)
        && nullptr != lob_manager_
        && (UINT64_MAX == source_byte_len_
            || source_byte_len_ <= OB_MAX_LONGTEXT_LENGTH_WITHOUT_LOB_HEADER);
  }
  TO_STRING_KV(KP_(read_ctx), KP_(lob_manager), K_(source_byte_len));

  ObLobReadContext *read_ctx_;
  ObLobManager *lob_manager_;
  // 当前读取范围预期返回的字节数；UINT64_MAX 表示未知，不做预期长度校验。
  uint64_t source_byte_len_;
};

// 描述目标 out-row LOB 的逻辑身份和内存生命周期。
struct ObLobTargetWriteCtx
{
  ObLobTargetWriteCtx()
    : locator_allocator_(nullptr), result_allocator_(nullptr),
      coll_type_(CS_TYPE_INVALID), lob_col_idx_(OB_INVALID_INDEX_INT64),
      lob_id_(), seq_no_(), lob_meta_tablet_id_(), external_lob_seq_id_(0)
  {}
  bool is_valid() const
  {
    return nullptr != locator_allocator_
        && nullptr != result_allocator_
        && locator_allocator_ != result_allocator_
        && ObCharset::is_valid_collation(coll_type_)
        && lob_col_idx_ >= 0
        && lob_id_.is_valid()
        && lob_id_.get_column_idx() == static_cast<uint64_t>(lob_col_idx_);
  }
  TO_STRING_KV(KP_(locator_allocator), KP_(result_allocator), K_(coll_type),
               K_(lob_col_idx), K_(lob_id), K_(seq_no), K_(lob_meta_tablet_id),
               K_(external_lob_seq_id));

  // locator 的生命周期必须覆盖目标主表 datum。
  ObIAllocator *locator_allocator_;
  // piece/meta 结果在当前 LOB drain 完成后即可整体复用。
  ObIAllocator *result_allocator_;
  ObCollationType coll_type_;
  int64_t lob_col_idx_;
  ObLobId lob_id_;
  transaction::ObTxSEQ seq_no_;
  common::ObTabletID lob_meta_tablet_id_;
  // External sequences consume a separate value from the target LOB ID cache.
  uint64_t external_lob_seq_id_;
};

// 一个 ObLobWriteIter 只处理一个目标 out-row LOB。
// 先流式返回 piece，再按 summary、chunk meta 的顺序返回 meta。
class ObLobWriteIter
{
private:
  enum class State : uint8_t
  {
    INVALID = 0,
    PIECE = 1,
    META_SUMMARY = 2,
    META_CHUNK = 3,
    END = 4
  };

public:
  ObLobWriteIter();
  ~ObLobWriteIter() { reset(); }
  int open(const ObLobSourceReadCtx &source_ctx,
           const ObLobTargetWriteCtx &target_ctx);
  int get_next_row(ObLobWriteResult &result);
  // 仅在全部 piece/meta 结果成功 drain 后返回最终 locator。
  int get_pending_locator(ObLobLocatorV2 &locator) const;
  void reset();
  bool is_finished() const { return State::END == state_; }

  TO_STRING_KV(K_(is_inited), K_(state), K_(source_byte_len), K_(write_byte_len),
               K_(write_char_len), K_(chunk_count), K_(piece_result_count),
               K_(meta_result_count), K_(meta_idx), K_(pending_locator));

private:
  int open_source_(const ObLobSourceReadCtx &source_ctx);
  int open_external_(const ObLobSourceReadCtx &source_ctx, const ObLobTargetWriteCtx &target_ctx);
  int build_pending_locator_(const ObLobTargetWriteCtx &target_ctx);
  int get_next_piece_(ObLobWriteResult &result);
  int build_chunk_write_result_(ObLobChunk &chunk, ObLobWriteResult &result);
  // 完成 piece 阶段，构造汇总 meta 并切换到 meta 输出阶段。
  int finish_piece_phase_();
  int get_next_meta_(ObLobWriteResult &result);
  int get_next_source_chunk_(ObLobChunk &chunk);
  int build_summary_();
  int add_chunk_stats_(const ObLobChunk &chunk);
  int check_write_result_() const;
  void reset_query_iter_();

private:
  bool is_inited_;
  State state_;
  ObIAllocator *result_allocator_;
  ObLobQueryIter *query_iter_;
  ObLobDiskLocatorBuilder locator_builder_;
  ObLobLocatorV2 pending_locator_;
  ObCollationType coll_type_;
  int64_t lob_col_idx_;
  ObLobId lob_id_;
  ObLobSeqIdGenerator seq_id_generator_;
  ObSEArray<ObLobMetaInfo, 16> chunk_meta_infos_;
  ObLobMetaInfo summary_info_;
  int64_t meta_idx_;
  uint64_t source_byte_len_;
  uint64_t write_byte_len_;
  uint64_t write_char_len_;
  uint64_t chunk_count_;
  uint64_t piece_result_count_;
  uint64_t meta_result_count_;

  DISALLOW_COPY_AND_ASSIGN(ObLobWriteIter);
};
} // storage
} // oceanbase

#endif

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_DDL_OB_LOB_MACRO_BLOCK_WRITER_
#define _OCEANBASE_STORAGE_DDL_OB_LOB_MACRO_BLOCK_WRITER_

#include "storage/ddl/ob_ddl_struct.h"
#include "share/ob_tablet_autoincrement_param.h" // for ObTabletCacheInterval
#include "storage/ddl/ob_ddl_seq_generator.h"
#include "storage/lob/store/ob_lob_meta.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/blocksstable/ob_macro_seq_generator.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/ddl/ob_ddl_tablet_context.h"

namespace oceanbase
{

namespace blocksstable
{
class ObStorageDatum;
}

namespace sql
{
class ObChunkDatumStore;
}

namespace storage
{
class ObDDLRedoLogWriterCallback;
class ObColumnSchemaItem;
class ObDDLIndependentDag;
class ObCgMacroBlockWriter;

// 每个列级 writer 使用独立适配器，实际序号统一从 parent slice 共享 generator 领取。
class ObLobAuxMacroSeqGenerator : public blocksstable::ObMacroSeqGenerator
{
public:
  ObLobAuxMacroSeqGenerator()
    : shared_seq_generator_(nullptr), current_(-1), reserved_current_(-1),
      reserved_next_(-1), has_reserved_next_(false)
  {}
  int bind(blocksstable::ObMacroIncSeqGenerator &shared_seq_generator);
  virtual void reset() override;
  virtual int init(const blocksstable::ObMacroSeqParam &seq_param) override;
  virtual int get_next(int64_t &seq_val) override;
  virtual int preview_next(const int64_t current_val, int64_t &next_val) const override;
  virtual int64_t get_current() override { return current_; }
  TO_STRING_KV(KP_(shared_seq_generator), K_(current), K_(reserved_current),
               K_(reserved_next), K_(has_reserved_next));

private:
  blocksstable::ObMacroIncSeqGenerator *shared_seq_generator_;
  int64_t current_;
  mutable int64_t reserved_current_;
  mutable int64_t reserved_next_;
  mutable bool has_reserved_next_;
};

class ObLobMacroBlockWriter
{
public:
  ObLobMacroBlockWriter();
  ~ObLobMacroBlockWriter();
  int init(const ObWriteMacroParam &param,
           const ObTabletID &data_tablet_id);
  int write(const ObColumnSchemaItem &column_schema, const int64_t lob_col_idx,
            ObIAllocator &row_allocator, blocksstable::ObStorageDatum &datum);
  int close();
  TO_STRING_KV(K(is_inited_), K(lob_meta_tablet_id_), K(lob_piece_tablet_id_),
      K(slice_idx_), K(lob_id_cache_), K(lob_column_count_), K(lob_id_generator_),
      K(use_staging_), K(staging_dir_id_), K(lob_meta_row_), K(lob_piece_row_),
      K(column_write_ctxs_));

private:
  // 单个 LOB 暂存区超过该软内存水位后触发溢写，与列存 CG 暂存水位保持一致。
  static constexpr int64_t LOB_STAGING_STORE_MEMORY_LIMIT = 512L << 10;
  // 暂存区的最小 block 大小，单行较大时会申请更大的 block。
  static constexpr int64_t LOB_STAGING_BLOCK_SIZE = 4L << 10;
  // 正常 LOB piece 不超过 256 KiB，预留四倍空间兼容行序列化开销和倍增扩容。
  static constexpr int64_t LOB_STAGING_ROW_BUFFER_REUSE_LIMIT =
      4 * ObLobMetaUtil::LOB_PIECE_DATA_SIZE;

  struct ObLobAuxRunState
  {
    ObLobAuxRunState()
      : expected_lob_id_start_(0), expected_lob_id_end_(0),
        last_written_lob_id_(0), has_last_written_lob_id_(false)
    {}
    void reset(
        const uint64_t expected_lob_id_start,
        const uint64_t expected_lob_id_end);
    int check_and_record(const ObLobId &lob_id, const int64_t lob_col_idx);
    TO_STRING_KV(K_(expected_lob_id_start), K_(expected_lob_id_end),
                 K_(last_written_lob_id), K_(has_last_written_lob_id));

    // StoreItem 声明的 LobId 范围，用于校验本 run 的实际写入。
    uint64_t expected_lob_id_start_;
    uint64_t expected_lob_id_end_;
    uint64_t last_written_lob_id_;
    bool has_last_written_lob_id_;
  };

  struct ObLobColumnWriteCtx
  {
    ObLobColumnWriteCtx()
      : lob_col_idx_(OB_INVALID_INDEX_INT64),
        meta_seq_generator_(), piece_seq_generator_(),
        meta_run_state_(), piece_run_state_(),
        meta_macro_writer_(nullptr), piece_macro_writer_(nullptr),
        meta_staging_store_(nullptr), piece_staging_store_(nullptr)
    {}
    TO_STRING_KV(K_(lob_col_idx), K_(meta_seq_generator),
        K_(piece_seq_generator), K_(meta_run_state),
        K_(piece_run_state), KP_(meta_macro_writer),
        KP_(piece_macro_writer), KP_(meta_staging_store),
        KP_(piece_staging_store));

    int64_t lob_col_idx_;
    ObLobAuxMacroSeqGenerator meta_seq_generator_;
    ObLobAuxMacroSeqGenerator piece_seq_generator_;
    ObLobAuxRunState meta_run_state_;
    ObLobAuxRunState piece_run_state_;
    ObCgMacroBlockWriter *meta_macro_writer_;
    ObCgMacroBlockWriter *piece_macro_writer_;
    // 暂存当前 LOB 列序列化后的 meta 行，供后续回放。
    sql::ObChunkDatumStore *meta_staging_store_;
    // 暂存当前 LOB 列序列化后的 piece 行，供后续回放。
    sql::ObChunkDatumStore *piece_staging_store_;
  };

  int init_shared_macro_seq_generator(
      const ObTabletID &tablet_id,
      blocksstable::ObMacroIncSeqGenerator &seq_generator,
      blocksstable::ObMacroDataSeq &start_sequence);
  int init_column_write_ctxs();
  int get_column_write_ctx(const int64_t lob_col_idx, ObLobColumnWriteCtx *&column_ctx);
  int drain_lob_iter(
      ObLobColumnWriteCtx &column_ctx,
      const transaction::ObTxSEQ &target_lob_seq_no,
      const bool skip_meta_write);
  int append_meta_row(ObLobColumnWriteCtx &column_ctx, const ObLobMetaInfo &meta_info);
  int append_piece_row(ObLobColumnWriteCtx &column_ctx, const ObLobPieceInfo &piece_info);
  int transform_lob_meta_row(const ObLobMetaInfo &meta_info);
  int transform_lob_piece_row(const ObLobPieceInfo &piece_info);
  int prepare_macro_block_writer(
      ObLobColumnWriteCtx &column_ctx,
      const ObDirectLoadTabletTarget target);
  int append_staging_row(
      ObLobColumnWriteCtx &column_ctx,
      const ObDirectLoadTabletTarget target,
      const blocksstable::ObDatumRow &row);
  int get_or_create_staging_store(
      ObLobColumnWriteCtx &column_ctx,
      const ObDirectLoadTabletTarget target,
      sql::ObChunkDatumStore *&staging_store);
  int spill_staging_store_if_needed(sql::ObChunkDatumStore &staging_store);
  int replay_staging_stores(const ObDirectLoadTabletTarget target);
  // 回放单个暂存区：调用方已按 target 选好 staging_store 与 seq_generator，
  // 内部按 finish_add_row -> prepare -> 迭代回放 -> close 的顺序处理。
  int replay_staging_store(
      ObLobColumnWriteCtx &column_ctx,
      const ObDirectLoadTabletTarget target,
      sql::ObChunkDatumStore &staging_store,
      ObLobAuxMacroSeqGenerator &seq_generator);
  void destroy_staging_store(sql::ObChunkDatumStore *&staging_store);
  void destroy_staging_stores();
  int switch_lob_id_cache();
  int close_macro_block_writers();
  int close_macro_block_writer(
      ObCgMacroBlockWriter *macro_block_writer,
      ObLobAuxMacroSeqGenerator &seq_generator);
  void destroy_macro_block_writers();

private:
  bool is_inited_;
  ObLSID ls_id_;
  ObTabletID tablet_id_;

  ObTabletID lob_meta_tablet_id_;
  ObTabletID lob_piece_tablet_id_;
  int64_t slice_idx_;
  int64_t lob_inrow_threshold_;
  uint64_t lob_id_interval_start_;
  uint64_t lob_id_interval_end_;

  share::ObTabletCacheInterval lob_id_cache_;
  int64_t lob_column_count_;
  // 多 LOB 列时，是否启用受内存限制的暂存并依次回放。
  bool use_staging_;
  ObDDLSeqGenerator lob_id_generator_;
  blocksstable::ObMacroIncSeqGenerator meta_shared_seq_generator_;
  blocksstable::ObMacroIncSeqGenerator piece_shared_seq_generator_;
  blocksstable::ObMacroDataSeq meta_start_sequence_;
  blocksstable::ObMacroDataSeq piece_start_sequence_;

  ObArenaAllocator lob_arena_;
  ObLobWriteIter lob_write_iter_;
  // 用于转换单条 LOB meta 记录的复用行。
  blocksstable::ObDatumRow lob_meta_row_;
  // 用于转换单条 LOB piece 记录的复用行。
  blocksstable::ObDatumRow lob_piece_row_;
  // 用于将单行序列化至暂存区的临时缓冲区。
  blocksstable::ObSelfBufferWriter staging_row_buffer_;
  // 依次回放各 LOB 列时复用的宏块写入器。
  ObCgMacroBlockWriter *replay_macro_writer_;
  // 所有暂存区共享的溢写目录 ID，-1 表示尚未分配。
  int64_t staging_dir_id_;

  ObSEArray<ObLobColumnWriteCtx, 4> column_write_ctxs_;
  // lob_col_idx is the storage column index, and the mapped value is the dense write context index.
  ObSEArray<int64_t, 4> lob_col_idx_to_write_ctx_idx_;
  ObWriteMacroParam param_;
  int64_t total_lob_cell_count_;
  int64_t inrow_lob_cell_count_;
};

} // end namespace storage
} // end namespace oceanbase

#endif//_OCEANBASE_STORAGE_DDL_OB_LOB_MACRO_BLOCK_WRITER_

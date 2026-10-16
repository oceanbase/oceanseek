/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEABASE_STORAGE_OB_LOB_PERSISTENT_ITERATOR_
#define OCEABASE_STORAGE_OB_LOB_PERSISTENT_ITERATOR_
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_range.h"
#include "storage/access/ob_dml_param.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/blocksstable/ob_datum_row_iterator.h"
#include "storage/lob/common/ob_lob_context.h"
#include "ob_lob_store_common.h"
#include "common/row/ob_row_iterator.h"
#include "ob_lob_meta.h"
#include "storage/lob/store/ob_lob_persistent_reader_cache.h"

namespace oceanbase
{
namespace storage
{

class ObLobQueryIter;
class ObPersistentLobAdaptor;
struct ObLobMetaScanResult;
class ObLobMetaScanIter;

struct ObLobAuxTableRowKey
{
public:
  static const int64_t ROW_KEY_COLUMN_CNT = 3;
  static const int64_t LOB_COL_IDX_IDX = 0;
  static const int64_t LOB_ID_IDX = 1;
  static const int64_t SEQ_ID_IDX = 2;
  void make_min_row_key();
  void make_max_row_key();
  void set_lob_col_idx(int64_t lob_col_idx);
  void set_lob_id(const char *lob_id_str, const int64_t lob_id_len);
  void set_seq_id(const char *seq_id_str, const int64_t seq_id_len);
  ObRowkey to_row_key() { return ObRowkey(row_key_, ROW_KEY_COLUMN_CNT); }
public:
  ObObj row_key_[ROW_KEY_COLUMN_CNT];
};

class ObLobAuxTableRangeBuilder
{
public:
  // full table scan: [min, min, min] ~ [max, max, max]
  int build_full_range(ObNewRange &range);
  // fix column_idx + lob_id, seq_id range is [start_seq_id, end_seq_id],
  // empty start_seq_id falls back to min,
  // empty end_seq_id falls back to max.
  int build_from_start_seq(int64_t column_idx,
                           const ObString &lob_id,
                           const ObString &start_seq_id,
                           const ObString &end_seq_id,
                           ObNewRange &range);
  // fix column_idx + lob_id + seq_id, single chunk get
  int build(int64_t column_idx, 
            const ObString &lob_id,
            const ObString &seq_id,
            ObNewRange &range);
private:
  int build(ObNewRange &range);
private:
  ObLobAuxTableRowKey min_row_key_;
  ObLobAuxTableRowKey max_row_key_;
};

// 为 LOB Meta 和 LOB Piece 辅助表提供扫描能力。open() 后可通过 rescan() 复用，
// 避免重复打开 table scan。该迭代器由 ObLobAccessCtx 申请、释放和缓存。
class ObLobAuxTableBaseIterator
{
public:
  ObLobAuxTableBaseIterator(const ObLobAccessCtx *access_ctx = nullptr)
    : is_opened_(),
      main_tablet_id_(),
      lob_meta_tablet_id_(),
      lob_piece_tablet_id_(),
      range_builder_(),
      seq_id_local_buf_(),
      scan_param_(),
      row_iter_(nullptr),
      adaptor_(nullptr),
      scan_allocator_("LobScan", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      access_ctx_(access_ctx)
  {
    MEMSET(seq_id_local_buf_, 0, sizeof(seq_id_local_buf_));
  }

  virtual ~ObLobAuxTableBaseIterator() { reset(); }

  bool is_open() const { return is_opened_; }
  int reset();
  int open(const LobAuxTarget target,
           ObLobReadContext &read_ctx,
           ObPersistentLobAdaptor *adaptor,
           ObIAllocator *scan_allocator);
  int open(ObTabletID &main_tablet_id, ObTabletID &lob_piece_tablet_id);
  int rescan(ObLobReadContext &read_ctx);
  int rescan(ObNewRange &range);
  ObTableScanParam &get_scan_param() { return scan_param_; }
  const ObLobAccessCtx *get_access_ctx() const { return access_ctx_; }

  VIRTUAL_TO_STRING_KV(
      K_(main_tablet_id),
      K_(lob_meta_tablet_id),
      K_(lob_piece_tablet_id),
      K_(seq_id_local_buf),
      KP_(row_iter),
      KPC_(row_iter),
      KP_(access_ctx));

protected:
  // @brief build scan range from param via range_builder_
  int build_range(const ObLobReadContext &read_ctx, ObNewRange &range);
  // @brief init scan
  int scan(const LobAuxTarget target,
           ObLobReadContext &read_ctx,
           const bool is_get,
           ObIAllocator *stmt_allocator,
           ObIAllocator *scan_allocator);
  // @brief reuse_scan_iter + table_rescan with current scan_param_
  int do_rescan(ObLobReadContext &read_ctx);
  int revert_scan_iter();

protected:
  bool is_opened_;
  // tablet id of main table
  ObTabletID main_tablet_id_;
  ObTabletID lob_meta_tablet_id_;
  ObTabletID lob_piece_tablet_id_;

  ObLobAuxTableRangeBuilder range_builder_;

  // used for single get
  char seq_id_local_buf_[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObTableScanParam scan_param_;
  // scan iter, must be released by calling access service revert_scan_iter
  ObNewRowIterator *row_iter_;
  ObPersistentLobAdaptor *adaptor_;
  ObArenaAllocator scan_allocator_;
  const ObLobAccessCtx *access_ctx_;

  DISALLOW_COPY_AND_ASSIGN(ObLobAuxTableBaseIterator);
};

template<typename T>
class ObLobAuxTableIterator : public ObLobAuxTableBaseIterator
{
public:
  ObLobAuxTableIterator(const ObLobAccessCtx *access_ctx)
    : ObLobAuxTableBaseIterator(access_ctx)
  {}
  virtual ~ObLobAuxTableIterator() {}

  int get_next_row(T &row);

  INHERIT_TO_STRING_KV("ObLobAuxTableBaseIterator", ObLobAuxTableBaseIterator, KP(this));

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobAuxTableIterator);
};

typedef ObLobAuxTableIterator<ObLobMetaInfo> ObLobMetaTableIter;
typedef ObLobAuxTableIterator<ObLobPieceInfo> ObLobPieceTableIter;

class ObLobPersistWriteIter : public blocksstable::ObDatumRowIterator
{
public:
    ObLobPersistWriteIter(): write_ctx_(nullptr) {}
    virtual ~ObLobPersistWriteIter() {}

    virtual int get_next_row(blocksstable::ObDatumRow *&row) override { return OB_NOT_IMPLEMENT; }

protected:
  ObLobWriteContext *write_ctx_;
};

class ObLobPersistInsertSingleRowIter: public ObLobPersistWriteIter
{
public:
  ObLobPersistInsertSingleRowIter():
    ObLobPersistWriteIter(),
    target_(LobAuxTarget::MAX),
    row_(nullptr),
    iter_end_(false)
  {}
  int init(ObLobInsertContext *insert_ctx,
           const LobAuxTarget target,
           blocksstable::ObDatumRow *row);

  virtual ~ObLobPersistInsertSingleRowIter() {}
  virtual int get_next_row(blocksstable::ObDatumRow *&row);
	virtual void reset() { iter_end_ = false; }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLobPersistInsertSingleRowIter);
private:
  // data members
  LobAuxTarget target_;
  blocksstable::ObDatumRow *row_;
  bool iter_end_;
};


class ObLobPersistDeleteSingleRowIter: public ObLobPersistWriteIter
{
public:
  ObLobPersistDeleteSingleRowIter():
    ObLobPersistWriteIter(),
    row_(nullptr),
    iter_end_(false)
  {}
  int init(ObLobDeleteContext *delete_ctx, blocksstable::ObDatumRow *row);

  virtual ~ObLobPersistDeleteSingleRowIter() {}
  virtual int get_next_row(blocksstable::ObDatumRow *&row);
	virtual void reset() { iter_end_ = false; }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLobPersistDeleteSingleRowIter);
private:
  // data members
  blocksstable::ObDatumRow *row_;
  bool iter_end_;
};

class ObLobPersistUpdateSingleRowIter : public ObLobPersistWriteIter
{
public:
  ObLobPersistUpdateSingleRowIter()
    : ObLobPersistWriteIter(),
      old_row_(nullptr),
      new_row_(nullptr),
      got_old_row_(false),
      is_iter_end_(false)
  {}

  virtual ~ObLobPersistUpdateSingleRowIter() {}

  int init(ObLobUpdateContext *update_ctx, blocksstable::ObDatumRow *old_row, blocksstable::ObDatumRow *new_row);

  virtual int get_next_row(blocksstable::ObDatumRow *&row) override;
  virtual void reset() override {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLobPersistUpdateSingleRowIter);

private:
  blocksstable::ObDatumRow *old_row_;
  blocksstable::ObDatumRow *new_row_;
  bool got_old_row_;
  bool is_iter_end_;
};

struct ObLobWriteStats
{
  ObLobWriteStats() { reset(); }

  void reset();
  int add_chunk(const ObLobChunk &chunk);

  TO_STRING_KV(K_(byte_len), K_(char_len), K_(chunk_count));

  uint64_t byte_len_;
  uint64_t char_len_;
  uint64_t chunk_count_;
};

class ObLobInTablePieceInsertIter final : public blocksstable::ObDatumRowIterator
{
public:
  ObLobInTablePieceInsertIter();
  virtual ~ObLobInTablePieceInsertIter() { reset(); }

  int init(ObLobQueryIter *query_iter,
           common::ObIAllocator *allocator,
           const int64_t chunk_size,
           const int64_t lob_col_idx,
           const common::ObLobId &lob_id,
           common::ObIArray<ObLobMetaInfo> *meta_array,
           ObLobWriteStats *write_stats);
  int set_start_seq_id(const common::ObString &start_seq_id);
  virtual int get_next_row(blocksstable::ObDatumRow *&row) override;
  virtual void reset() override;

  bool is_end() const { return is_end_; }

  TO_STRING_KV(K_(is_inited), K_(is_end), KP_(query_iter), K_(chunk_size),
               K_(lob_col_idx), K_(lob_id), KPC_(write_stats));

private:
  bool is_inited_;
  bool is_end_;
  ObLobQueryIter *query_iter_;
  common::ObIAllocator *allocator_;
  int64_t chunk_size_;
  int64_t lob_col_idx_;
  common::ObLobId lob_id_;
  common::ObIArray<ObLobMetaInfo> *meta_array_;
  ObLobWriteStats *write_stats_;
  common::ObString data_meta_lob_info_;
  ObLobSeqIdGenerator seq_id_gen_;
  ObLobChunk chunk_;
  ObLobPieceInfo piece_info_;
  blocksstable::ObDatumRow row_;

  DISALLOW_COPY_AND_ASSIGN(ObLobInTablePieceInsertIter);
};

class ObLobMetaTableBatchInsertIter : public blocksstable::ObDatumRowIterator
{
public:
  ObLobMetaTableBatchInsertIter() { reset(); }
  int init(ObIArray<ObLobMetaInfo> *array);
  virtual void reset() override;
  virtual int get_next_row(blocksstable::ObDatumRow *&row) override;

  TO_STRING_KV(K(is_inited_), KPC(array_), K(idx_));
private:
  bool is_inited_;
  ObIArray<ObLobMetaInfo> *array_;
  int64_t idx_;
  blocksstable::ObDatumRow row_;
};

template<typename T>
class ObLobAuxTableBatchDeleteIterator : public blocksstable::ObDatumRowIterator
{
public:
  ObLobAuxTableBatchDeleteIterator()
    : is_inited_(false),
      delete_ctx_(nullptr),
      scan_iter_(nullptr),
      row_()
  {}
  virtual ~ObLobAuxTableBatchDeleteIterator() { reset(); }
  virtual void reset() override;
  int init(ObLobDeleteContext *delete_ctx, ObLobAuxTableIterator<T> *scan_iter);
  virtual int get_next_row(blocksstable::ObDatumRow *&row) override;

private:
  bool is_inited_;
  ObLobDeleteContext *delete_ctx_;
  ObLobAuxTableIterator<T> *scan_iter_;
  T row_info_;
  blocksstable::ObDatumRow row_;
};

} // storage
} // oceanbase

#endif

/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_LOB_CONTEXT_H_
#define OCEANBASE_STORAGE_OB_LOB_CONTEXT_H_

#include "common/ob_tablet_id.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "lib/net/ob_addr.h"
#include "lib/string/ob_string.h"
#include "share/ob_ls_id.h"
#include "share/schema/ob_schema_struct.h"
#include "share/scn.h"
#include "lib/alloc/alloc_struct.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_print_utils.h"
#include "object/ob_object.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "ob_lob_locator.h"
#include "storage/blocksstable/ob_storage_datum.h"

namespace oceanbase
{

namespace blocksstable {
class ObDatumRow;
}

namespace storage
{

class ObDMLBaseParam;
struct ObDMLRunningCtx;
class ObLobAccessCtx;
struct ObLobMetaInfo;
struct ObOuttableLobControlKey;

// ObLobContextBase 是 ObLobReadContext 和 ObLobWriteContext 的公共基类，包含
// 两者共享的参数。它只能通过派生 context 构造、赋值和析构。
struct ObLobContextBase
{
public:
  bool is_char() const { return coll_type_ != common::ObCollationType::CS_TYPE_BINARY; }
  bool is_across_tenant() const;
  uint16_t column_idx() const { return store_column_idx_; }

  TO_STRING_KV(KP(this), K_(tenant_id), K_(ls_id), K_(tablet_id),
      K_(lob_meta_tablet_id), K_(lob_piece_tablet_id), K_(coll_type),
      K_(src_loc), K_(byte_size), K_(timeout), K_(offset), K_(len),
      KP_(allocator), K_(snapshot), K_(fb_snapshot), K_(read_latest),
      K_(need_read_latest), K_(store_column_idx), K_(addr),
      K_(src_loc_wrapper));

protected:
  ObLobContextBase();
  ~ObLobContextBase() = default;
  int init_src_loc_wrapper_();
  int assign_common(const ObLobContextBase &other);
  virtual void reset();

public:
  common::ObIAllocator *allocator_;
  transaction::ObTxReadSnapshot snapshot_;
  ObSQLMode sql_mode_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID lob_meta_tablet_id_;
  common::ObTabletID lob_piece_tablet_id_;
  common::ObCollationType coll_type_;
  common::ObLobLocatorV2 src_loc_;
  uint64_t byte_size_;
  int64_t timeout_;
  share::SCN fb_snapshot_;
  uint64_t offset_;
  uint64_t len_;
  bool read_latest_;
  bool need_read_latest_;
  ObLobDiskLocatorWrapper src_loc_wrapper_;
  uint16_t store_column_idx_;
  common::ObAddr addr_;
};

// Read-only state passed through LOB query, retry, RPC, and persistent scan paths.
struct ObLobReadContext : public ObLobContextBase
{
public:
  ObLobReadContext();
  ~ObLobReadContext();
  void reset() override;

  int build(common::ObLobLocatorV2 &lob_locator,
            common::ObIAllocator &allocator,
            uint64_t offset,
            uint64_t len,
            transaction::ObTxDesc *tx_desc = nullptr);
  // 根据存储层 DML 上下文和行外 LOB datum 构造支持本地、远程重试的读上下文。
  // allow_none_read_snapshot 为 true 时，将 none_read 快照回退为当前 LS 的 MVCC 快照。
  int build_for_dml(const ObDMLRunningCtx &run_ctx,
                    const int16_t col_idx,
                    const blocksstable::ObStorageDatum &datum,
                    const bool allow_none_read_snapshot);
  int assign(const ObLobReadContext &read_ctx);

  bool is_remote() const;
  // Physical LOB type from the source locator, independent of the read collation.
  bool is_get() const
  {
    return !is_full_table_scan_ && !seq_id_start_.empty() && seq_id_start_ == seq_id_end_;
  }
  bool enable_block_cache() const;

  INHERIT_TO_STRING_KV("ObLobContextBase", ObLobContextBase,
      K_(scan_backward), K_(from_rpc), K_(inrow_read_nocopy),
      K_(enable_remote_retry), K_(is_full_table_scan),
      KP_(access_ctx), K_(seq_id_start), K_(seq_id_end));

private:
  int get_tx_read_snapshot_(common::ObLobLocatorV2 &locator,
                            transaction::ObTxReadSnapshot &read_snapshot);
  int set_tx_read_snapshot_(common::ObLobLocatorV2 &locator,
                            transaction::ObTxDesc *tx_desc);

public:
  bool scan_backward_;
  bool from_rpc_;
  bool inrow_read_nocopy_;
  bool enable_remote_retry_;
  bool is_full_table_scan_;
  ObLobAccessCtx *access_ctx_;
  common::ObString seq_id_start_;
  common::ObString seq_id_end_;
  // iter_allocator_ only used for query iter, can be reset in retry process
  common::ObArenaAllocator iter_allocator_;
};

enum class ObLobDmlOp
{
  INVALID = 0,
  INSERT = 1,
  UPDATE = 2,
  DELETE = 3,
};

// distinguish target between meta and piece aux tables
enum class LobAuxTarget : uint8_t
{
  META = 0,
  PIECE = 1,
  MAX
};

inline bool is_valid_lob_aux_target(const LobAuxTarget target)
{
  return target >= LobAuxTarget::META && target < LobAuxTarget::MAX;
}

// for logging; returns a static string, safe to use with K() macro
inline const char *get_lob_aux_target_str(const LobAuxTarget target)
{
  const char *str = "UNKNOWN";
  switch (target) {
    case LobAuxTarget::META:
      str = "lob meta";
      break;
    case LobAuxTarget::PIECE:
      str = "lob piece";
      break;
    default:
      str = "UNKNOWN";
      break;
  }
  return str;
}

struct ObLobWriteContext : public ObLobContextBase
{
public:
  ObLobWriteContext();
  virtual ~ObLobWriteContext();

public:
  int assign(const ObLobWriteContext &other);
  bool is_valid();

  virtual int build(const ObDMLRunningCtx &run_ctx,
                    const blocksstable::ObDatumRow &data_row,
                    const int16_t col_idx,
                    const ObLobLocatorV2 &src_loc);

  virtual int build_for_dml(const ObDMLRunningCtx &run_ctx,
                            const blocksstable::ObDatumRow &data_row,
                            const int16_t col_idx,
                            const blocksstable::ObStorageDatum &datum);

  // 从 ObLobReadContext 复制公共参数；tx_desc_、DML param 等写请求独占参数需单独设置。
  int assign_source(const ObLobReadContext &source);
  int build_read_context(ObLobReadContext &read_ctx) const;
  int is_timeout() const;
  bool is_blob() { return coll_type_ == common::ObCollationType::CS_TYPE_BINARY; }

  bool is_remote() const;
  bool can_write_outtable() const;

  virtual int64_t get_chunk_size(const bool is_outtable) const;

  int64_t get_inrow_threshold() const;
  int64_t get_intable_threshold() const;

  int update_seq_no(const LobAuxTarget target);

  bool skip_flush_redo() const { return !try_flush_redo_; }

  int get_aux_dml_base_param(const LobAuxTarget target, ObDMLBaseParam *&base_param) const;
  int set_aux_dml_base_param(const LobAuxTarget target, ObDMLBaseParam *value);
  int get_aux_tablet_id(const LobAuxTarget target, common::ObTabletID &tablet_id) const;
  int get_aux_column_cnt(const LobAuxTarget target, uint64_t &column_cnt) const;
  int get_aux_rowkey_col_cnt(const LobAuxTarget target, uint64_t &rowkey_col_cnt) const;
  // Get the transaction context for writing OutTable CLOG. The LOB Meta DML base parameter must be initialized first.
  int get_trans_ctx(transaction::ObPartTransCtx *&tx_ctx) const;

  INHERIT_TO_STRING_KV("ObLobContextBase", ObLobContextBase,
      K(dest_loc_), K_(dml_op), K_(parent_seq_no), K_(insert_lob_seq_no),
      K_(update_lob_seq_no), K_(delete_lob_seq_no), K_(is_total_quantity_log),
      K_(inrow_threshold), K_(is_mlog),
      K_(try_flush_redo), K_(is_index_table), K_(table_def_type), KPC_(data_row),
      K_(is_table_without_pk), K_(intable_threshold), K_(allow_outtable_lob));

public:
  ObArenaAllocator own_allocator_;
  ObLobDmlOp dml_op_;
  // transaction
  transaction::ObTxDesc *tx_desc_; // for insert/update/delete

  // 写入 meta 和 piece 表所需的 DML 参数，由 allocator_ 分配。
  ObDMLBaseParam *dml_base_meta_param_;
  ObDMLBaseParam *dml_base_piece_param_;
  common::ObLobLocatorV2 dest_loc_; // should set by set_lob_locator

  // 父 DML 操作的事务序号，用于派生 LOB 辅助表写入序号。
  transaction::ObTxSEQ parent_seq_no_;
  // 辅助表 INSERT 操作使用的事务序号。
  transaction::ObTxSEQ insert_lob_seq_no_;
  // LOB_WRITEAPPEND 原地更新尾块、元信息或摘要时使用。
  transaction::ObTxSEQ update_lob_seq_no_;
  // 辅助表 DELETE 操作使用的事务序号。
  transaction::ObTxSEQ delete_lob_seq_no_;

  // true is full mode of binlog_row_image
  // default should be true, beacuse obcdc need old_row when delete
  bool is_total_quantity_log_;
  bool is_mlog_;
  // 是否主动刷 redo，若为 false，则交给后续主表流程主动刷新 redo
  bool try_flush_redo_;
  bool is_index_table_;
  share::schema::ObTableDefType table_def_type_;
  bool is_table_without_pk_;
  int64_t inrow_threshold_;
  int64_t intable_threshold_;
  const blocksstable::ObDatumRow *data_row_; // for tablet split
  common::ObIArray<ObOuttableLobControlKey> *outtable_lob_control_keys_;
  bool allow_outtable_lob_;
};

struct ObLobInsertContext : public ObLobWriteContext
{
public:
  virtual int build(const ObDMLRunningCtx &run_ctx,
                    const blocksstable::ObDatumRow &data_row,
                    const int16_t col_idx,
                    const ObLobLocatorV2 &src_loc) override;
  virtual int build_for_dml(const ObDMLRunningCtx &run_ctx,
                            const blocksstable::ObDatumRow &data_row,
                            const int16_t col_idx,
                            const blocksstable::ObStorageDatum &datum) override;
};

struct ObLobDeleteContext : public ObLobWriteContext
{
public:
  virtual int build_for_dml(const ObDMLRunningCtx &run_ctx,
                            const blocksstable::ObDatumRow &data_row,
                            const int16_t col_idx,
                            const blocksstable::ObStorageDatum &datum) override;
  virtual int build(const ObDMLRunningCtx &run_ctx,
                    const blocksstable::ObDatumRow &data_row,
                    const int16_t col_idx,
                    const ObLobLocatorV2 &src_loc) override;
};

struct ObLobUpdateContext : public ObLobInsertContext
{
public:
  ObLobUpdateContext() : lob_id_(), old_loc_() {}
  ~ObLobUpdateContext() {}

  virtual int build_for_dml(const ObDMLRunningCtx &run_ctx,
                            const blocksstable::ObDatumRow &data_row,
                            const int16_t col_idx,
                            const blocksstable::ObStorageDatum &datum) override;

public:
  INHERIT_TO_STRING_KV("ObLobWriteContext", ObLobWriteContext, K(lob_id_), K(old_loc_));
  // valid only when old value is outrow, reused as new lob_id for outrow->outrow update
  ObLobId lob_id_;
  // old value locator, kept for deleting old lob data in update_after
  common::ObLobLocatorV2 old_loc_;
};

struct ObLobAppendContext : public ObLobUpdateContext
{
public:
  ObLobAppendContext()
    : ObLobUpdateContext(),
      delta_loc_(),
      next_seq_id_(),
      update_first_(false),
      cur_byte_length_(0),
      cur_char_length_(0),
      cur_chunk_count_(0),
      total_byte_length_(0),
      total_char_length_(0),
      is_main_table_rowkey_col_(false)
  {}
  ~ObLobAppendContext() {}

  int build_for_dml(const ObDMLRunningCtx &run_ctx,
                    const blocksstable::ObDatumRow &data_row,
                    const int16_t col_idx,
                    const blocksstable::ObStorageDatum &old_datum,
                    const blocksstable::ObStorageDatum &new_datum);
  int build(const ObDMLRunningCtx &run_ctx,
            const blocksstable::ObDatumRow &data_row,
            const int16_t col_idx,
            const common::ObLobLocatorV2 &old_loc,
            const common::ObLobLocatorV2 &delta_loc);

public:
  INHERIT_TO_STRING_KV("ObLobUpdateContext", ObLobUpdateContext,
                       K(delta_loc_), K(next_seq_id_),
                       K(update_first_), K(cur_byte_length_), K(cur_char_length_),
                       K(cur_chunk_count_),
                       K(total_byte_length_), K(total_char_length_),
                       K(is_main_table_rowkey_col_));
  common::ObLobLocatorV2 delta_loc_;
  common::ObString next_seq_id_;
  bool update_first_;
  uint64_t cur_byte_length_;
  uint64_t cur_char_length_;
  uint64_t cur_chunk_count_;
  uint64_t total_byte_length_;
  uint64_t total_char_length_;
  bool is_main_table_rowkey_col_;
};

struct ObLobCompareParams
{

  ObLobCompareParams()
    : collation_left_(CS_TYPE_INVALID),
      collation_right_(CS_TYPE_INVALID),
      offset_left_(0),
      offset_right_(0),
      compare_len_(0),
      timeout_(0),
      tx_desc_(nullptr)
  {
  }

  TO_STRING_KV(K(collation_left_),
               K(collation_right_),
               K(offset_left_),
               K(offset_right_),
               K(compare_len_),
               K(timeout_),
               K(tx_desc_));

  ObCollationType collation_left_;
  ObCollationType collation_right_;
  uint64_t offset_left_;
  uint64_t offset_right_;

  // compare length
  uint64_t compare_len_;
  int64_t timeout_;
  transaction::ObTxDesc *tx_desc_;
};

struct ObLobStorageParam
{
  ObLobStorageParam():
    inrow_threshold_(OB_DEFAULT_LOB_INROW_THRESHOLD),
    is_rowkey_col_(false),
    is_index_table_(false)
  {}

  TO_STRING_KV(K_(inrow_threshold), K_(is_rowkey_col), K_(is_index_table));

  int64_t inrow_threshold_;
  bool is_rowkey_col_;
  bool is_index_table_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_CONTEXT_H_

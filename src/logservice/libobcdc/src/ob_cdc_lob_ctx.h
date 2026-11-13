/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIBOBCDC_LOB_CTX_H_
#define OCEANBASE_LIBOBCDC_LOB_CTX_H_

#include "common/object/ob_object.h"           // ObLobData, ObLobDataOutRowCtx
#include "storage/blocksstable/ob_datum_row.h" // ObDmlRowFlag
#include "lib/atomic/ob_atomic.h"              // ATOMIC_**
#include "lib/allocator/ob_allocator.h"
#include "lib/ob_define.h"
#include "storage/lob/common/ob_lob_locator.h"
#include "storage/lob/common/ob_lob_seq.h"
#include "ob_log_lighty_list.h"                // LightyList

namespace oceanbase
{
namespace libobcdc
{
struct ObLobImageAssembleDesc final
{
  ObLobImageAssembleDesc() { reset(); }
  ~ObLobImageAssembleDesc() { reset(); }

  void reset()
  {
    is_valid_ = false;
    lob_id_.reset();
    lob_operation_seq_no_.reset();
  }

  int init(const common::ObLobId &lob_id,
      const transaction::ObTxSEQ &lob_operation_seq_no);

  bool is_valid() const { return is_valid_; }

  TO_STRING_KV(K_(is_valid), K_(lob_id), K_(lob_operation_seq_no));

  bool is_valid_;
  common::ObLobId lob_id_;
  transaction::ObTxSEQ lob_operation_seq_no_;
};

struct ObLobColCtx final
{
public:
  ObLobColCtx() { reset(); }
  ~ObLobColCtx() { reset(); }

  void reset()
  {
    fragment_cb_array_ = nullptr;
    lob_column_value_.reset();
    col_ref_cnt_ = 0;
    expected_byte_len_ = 0;
    chunk_count_ = 0;
    padding_ = false;
  }

  int init(
      const uint64_t chunk_count,
      const uint64_t expected_byte_len,
      common::ObIAllocator &allocator);

  int set_col_value(
      const char *buf,
      const uint64_t buf_len);

  common::ObString &get_lob_column_value() { return lob_column_value_; }

  void set_col_ref_cnt(const uint64_t col_ref_cnt) { ATOMIC_SET(&col_ref_cnt_, col_ref_cnt); }
  uint64_t dec_col_ref_cnt() { return ATOMIC_SAF(&col_ref_cnt_, 1); }
  uint64_t get_col_ref_cnt() const { return ATOMIC_LOAD(&col_ref_cnt_); }
  bool is_padding() const { return ATOMIC_LOAD(&padding_); }
  void mark_padding() { ATOMIC_STORE(&padding_, true); }

  TO_STRING_KV("col_ref_cnt", get_col_ref_cnt(), K_(expected_byte_len), K_(chunk_count),
      "padding", is_padding());

public:
  common::ObString **fragment_cb_array_;
  common::ObString lob_column_value_;
  uint64_t col_ref_cnt_;
  // 完整 LOB image 的预期字节数，Merger 用它分配拼接缓冲区以及校验所有分片的总长度。
  uint64_t expected_byte_len_;
  // LOB image 的分片数量，用于分配分片槽位、初始化引用计数、控制拼接，以及 ResourceCollector 精确清理 Piece。
  uint64_t chunk_count_;

private:
  bool padding_;
};

struct ObLobDataGetCtx final
{
  ObLobDataGetCtx() { reset(); }
  ~ObLobDataGetCtx() { reset(); }

  void reset();

  int reset(
      void *host,
      const uint64_t column_id,
      const blocksstable::ObDmlRowFlag &dml_flag,
      const common::ObLobId &lob_id,
      const transaction::ObTxSEQ &lob_operation_seq_no,
      const bool is_new_col);

  int set_image_desc(
      const common::ObLobId &lob_id,
      const transaction::ObTxSEQ &lob_operation_seq_no,
      const bool is_new_col);

  bool has_image_desc(const bool is_new_col) const
  {
    return is_new_col ? new_image_desc_.is_valid() : old_image_desc_.is_valid();
  }

  const ObLobImageAssembleDesc &get_image_desc(const bool is_new_col) const
  {
    return is_new_col ? new_image_desc_ : old_image_desc_;
  }

  ObLobColCtx &get_lob_col_ctx(const bool is_new_col)
  {
    return is_new_col ? new_lob_col_ctx_ : old_lob_col_ctx_;
  }

  bool is_force_padding_by_append() const { return ATOMIC_LOAD(&force_padding_by_append_); }
  void set_force_padding_by_append() { ATOMIC_STORE(&force_padding_by_append_, true); }

  common::ObString **get_fragment_cb_array(const bool is_new_col)
  {
    common::ObString **res_str = nullptr;

    if (is_new_col) {
      res_str = new_lob_col_ctx_.fragment_cb_array_;
    } else {
      res_str = old_lob_col_ctx_.fragment_cb_array_;
    }

    return res_str;
  }

  uint64_t dec_col_ref_cnt(const bool is_new_col)
  {
    uint64_t ref_cnt = 0;

    if (is_new_col) {
      ref_cnt = new_lob_col_ctx_.dec_col_ref_cnt();
    } else {
      ref_cnt = old_lob_col_ctx_.dec_col_ref_cnt();
    }

    return ref_cnt;
  }

  int set_col_value(const bool is_new_col, const char *buf, const uint64_t buf_len);
  common::ObString &get_new_lob_column_value() { return new_lob_col_ctx_.get_lob_column_value(); }
  common::ObString &get_old_lob_column_value() { return old_lob_col_ctx_.get_lob_column_value(); }

  ObLobDataGetCtx *get_next() { return next_; }
  void set_next(ObLobDataGetCtx *next) { next_ = next; }
  void inc_lob_col_value_count(bool &is_lob_col_value_handle_done);

  int64_t to_string(char *buf, const int64_t buf_len) const;

  void *host_;    // ObLobDataOutRowCtxList
  uint64_t column_id_;
  blocksstable::ObDmlRowFlag dml_flag_;
  ObLobImageAssembleDesc new_image_desc_;
  ObLobImageAssembleDesc old_image_desc_;
  int8_t lob_col_value_handle_done_count_;
  ObLobColCtx new_lob_col_ctx_;
  ObLobColCtx old_lob_col_ctx_;
  ObLobDataGetCtx *next_;
  bool force_padding_by_append_;
};

typedef LightyList<ObLobDataGetCtx> ObLobDataGetCtxList;

enum class ObLobMergerTaskType
{
  PREPARE = 0,
  FRAGMENT = 1,
};

struct ObLobMergerTask
{
  explicit ObLobMergerTask(const ObLobMergerTaskType type) : type_(type) {}
  virtual ~ObLobMergerTask() {}
  TO_STRING_KV(K_(type));
  ObLobMergerTaskType type_;
};

// One Prepare task coordinates both old and new images of a LOB column, starting with new.
struct ObLobImagePrepareTask final : public ObLobMergerTask
{
  explicit ObLobImagePrepareTask(ObLobDataGetCtx &host) :
    ObLobMergerTask(ObLobMergerTaskType::PREPARE), host_(host) {}
  ~ObLobImagePrepareTask() override {}
  bool is_valid() const { return host_.has_image_desc(true) || host_.has_image_desc(false); }
  TO_STRING_KV(K_(type), K_(host));
  ObLobDataGetCtx &host_;
};

struct ObLobColFragTask final : public ObLobMergerTask
{
  ObLobColFragTask(ObLobDataGetCtx &host) :
    ObLobMergerTask(ObLobMergerTaskType::FRAGMENT), host_(host),
    is_new_col_(0),
    seq_id_(),
    idx_(UINT64_MAX)
  {}
  ~ObLobColFragTask() override {}

  void reset(
    const bool is_new_col,
    const common::ObString &seq_id,
    const uint64_t idx)
  {
    is_new_col_ = is_new_col;
    seq_id_ = seq_id;
    idx_ = idx;
  }
  bool is_valid() const
  {
    return host_.has_image_desc(is_new_col_/*is_new_col*/)
        && storage::ObLobSeqId(seq_id_).is_valid()
        && (UINT64_MAX != idx_);
  }

  TO_STRING_KV(
      K_(host),
      K_(is_new_col),
      "seq_id", storage::ObLobSeqId(seq_id_),
      K_(idx));

  ObLobDataGetCtx &host_;
  int8_t is_new_col_ : 1;
  common::ObString seq_id_;
  uint64_t idx_;
};

class IStmtTask;
class ObLobDataOutRowCtxList final
{
public:
  ObLobDataOutRowCtxList(common::ObIAllocator &allocator) :
    allocator_(allocator),
    is_ddl_(0),
    stmt_task_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID),
    aux_lob_meta_table_id_(OB_INVALID_ID),
    aux_lob_piece_table_id_(OB_INVALID_ID),
    lob_data_get_ctxs_(),
    lob_col_get_succ_count_(0)
  {
  }
  ~ObLobDataOutRowCtxList() { reset(); }

  void reset()
  {
    is_ddl_ = 0;
    stmt_task_ = nullptr;
    tenant_id_ = OB_INVALID_TENANT_ID;
    trans_id_.reset();
    aux_lob_meta_table_id_ = OB_INVALID_ID;
    aux_lob_piece_table_id_ = OB_INVALID_ID;
    lob_data_get_ctxs_.reset();
    lob_col_get_succ_count_ = 0;
  }

  void reset(
      IStmtTask *dml_stmt_task,
      const uint64_t tenant_id,
      const transaction::ObTransID &trans_id,
      const uint64_t aux_lob_meta_table_id,
      const uint64_t aux_lob_piece_table_id,
      const bool is_ddl);

  bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_
      && trans_id_.is_valid()
      && common::OB_INVALID_ID != aux_lob_meta_table_id_
      && common::OB_INVALID_ID != aux_lob_piece_table_id_
      && get_total_lob_count() > 0;
  }

  common::ObIAllocator &get_allocator() { return allocator_; }
  bool is_ddl() const { return is_ddl_; }
  IStmtTask *get_stmt_task() { return stmt_task_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  const transaction::ObTransID &get_trans_id() const { return trans_id_; }
  uint64_t get_aux_lob_meta_table_id() const { return aux_lob_meta_table_id_; }
  uint64_t get_aux_lob_piece_table_id() const { return aux_lob_piece_table_id_; }

  int64_t get_total_lob_count() const { return lob_data_get_ctxs_.num_; }
  bool has_out_row_lob() const { return get_total_lob_count() > 0; }
  ObLobDataGetCtxList &get_lob_data_get_ctx_list() { return lob_data_get_ctxs_; }
  void add(ObLobDataGetCtx *lob_data_get_ctx) { lob_data_get_ctxs_.add(lob_data_get_ctx); }
  int set_old_image_desc(
      const uint64_t column_id,
      const common::ObLobId &lob_id,
      const transaction::ObTxSEQ &lob_operation_seq_no);
  int get_lob_column_value(
      const uint64_t column_id,
      const bool is_new_col,
      common::ObString *&col_str);
  int get_lob_data_get_ctx(
      const uint64_t column_id,
      ObLobDataGetCtx *&result);

  bool is_all_lob_callback_done() const { return get_total_lob_count() == ATOMIC_LOAD(&lob_col_get_succ_count_); }
  void inc_lob_col_count(bool &is_all_lob_col_handle_done)
  {
    const int64_t total_lob_count = get_total_lob_count();
    const int64_t lob_col_get_succ_count = ATOMIC_AAF(&lob_col_get_succ_count_, 1);
    is_all_lob_col_handle_done = (total_lob_count == lob_col_get_succ_count);
  }

public:
  int64_t to_string(char *buf, const int64_t buf_len) const;

private:
  common::ObIAllocator &allocator_;
  int8_t is_ddl_ : 1;
  IStmtTask *stmt_task_;    // primary table row
  uint64_t tenant_id_;
  transaction::ObTransID trans_id_;
  uint64_t aux_lob_meta_table_id_;
  uint64_t aux_lob_piece_table_id_;
  ObLobDataGetCtxList lob_data_get_ctxs_;    // Table may contains one or more LOB columns
  int64_t lob_col_get_succ_count_;

  DISALLOW_COPY_AND_ASSIGN(ObLobDataOutRowCtxList);
};

} // namespace libobcdc
} // namespace oceanbase

#endif

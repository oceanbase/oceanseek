/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_LOB_HANDLER_H_
#define OCEANBASE_STORAGE_OB_LOB_HANDLER_H_

#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/store/ob_lob_aux_manager.h"
#include "storage/lob/store/ob_lob_meta.h"

namespace oceanbase
{
namespace storage
{

class ObLobMetaManager;
class ObLobOutRowQueryIter;
class ObLobManager;
struct ObLobWriteStats;

class ObLobQueryBaseHandler
{
public:
  ObLobQueryBaseHandler(ObLobReadContext &read_ctx):
    is_inited_(false),
    read_ctx_(read_ctx),
    lob_meta_mngr_(nullptr)
  {}

  int execute();

protected:
  virtual int do_execute() = 0;
  int init_base(ObLobMetaManager *lob_meta_mngr);

protected:
  bool is_inited_;
  ObLobReadContext &read_ctx_;
  ObLobMetaManager *lob_meta_mngr_;
};


// @brief 获取迭代 Lob 数据的迭代器，迭代器由 ob_malloc 分配内存，需要调用者负责释放
class ObLobQueryIterHandler : public ObLobQueryBaseHandler
{
public:
  ObLobQueryIterHandler(ObLobReadContext &read_ctx) :
    ObLobQueryBaseHandler(read_ctx),
    result_(nullptr)
  {}

  int init(ObLobMetaManager *lob_meta_mngr);

protected:
  virtual int do_execute();

public:
  ObLobOutRowQueryIter *result_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobQueryIterHandler);
};

// @brief 获取 Lob 完整数据
class ObLobQueryDataHandler : public ObLobQueryBaseHandler
{
public:
  ObLobQueryDataHandler(ObLobReadContext &read_ctx, ObString &data_buffer) :
    ObLobQueryBaseHandler(read_ctx),
    result_(data_buffer)
  {}

  int init(ObLobMetaManager *lob_meta_mngr);

protected:
  virtual int do_execute();

public:
   ObString &result_;
  
private:
  DISALLOW_COPY_AND_ASSIGN(ObLobQueryDataHandler);
};

// @brief 查询 Lob 的长度
class ObLobQueryLengthHandler : public ObLobQueryBaseHandler
{
public:
  enum QueryType {
    BYTE_LENGTH = 0,
    CHAR_LENGTH = 1
  };
  ObLobQueryLengthHandler(ObLobReadContext &read_ctx) :
    ObLobQueryBaseHandler(read_ctx),
    result_(0)
  {}

  int init(ObLobMetaManager *lob_meta_mngr, QueryType query_type);

protected:
  virtual int do_execute();

public:
  QueryType query_type_;
  uint64_t result_;
  
private:
  DISALLOW_COPY_AND_ASSIGN(ObLobQueryLengthHandler);
};

// 获取 lob meta 表中 seq_id=0 的 Summary 行。
class ObLobQueryInfoHandler : public ObLobQueryBaseHandler
{
public:
  ObLobQueryInfoHandler(
      ObLobReadContext &read_ctx,
      ObLobMetaInfo &lob_info)
    : ObLobQueryBaseHandler(read_ctx),
      result_(lob_info)
  {}

  int init(ObLobMetaManager *lob_meta_mngr);

protected:
  virtual int do_execute();

public:
  ObLobMetaInfo &result_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobQueryInfoHandler);
};

class ObLobInserter
{
public:
  ObLobInserter() { reset(); }
  ~ObLobInserter() { reset(); }
  void reset();
  int init(ObLobInsertContext *ctx,
           ObLobMetaManager *meta_mgr,
           ObLobPieceManager *piece_mgr);
  int insert();

  TO_STRING_KV(K(is_inited_), KPC(ctx_), KPC(meta_mgr_), KPC(piece_mgr_));
private:
  int write_meta_summary_(const common::ObLobId &lob_id,
                          const uint64_t total_byte_len,
                          const uint64_t total_char_len,
                          const uint64_t chunk_count);
  // @brief 获取流式输入的 input，以迭代器方式返回，并顺便获取预计输入的长度，用于写后校验长度
  int open_src_lob_query_(ObLobQueryIter *&query_iter,
                          uint64_t &source_byte_len);
private:
  bool is_inited_;
  ObLobInsertContext *ctx_;
  ObLobMetaManager *meta_mgr_;
  ObLobPieceManager *piece_mgr_;
};

class ObLobAppender
{
public:
  ObLobAppender() { reset(); }
  ~ObLobAppender() { reset(); }
  void reset();
  int init(ObLobAppendContext *ctx,
           ObLobMetaManager *meta_mgr,
           ObLobPieceManager *piece_mgr);
  int append();

  TO_STRING_KV(K(is_inited_), KPC(ctx_), KPC(meta_mgr_), KPC(piece_mgr_));
private:
  static bool is_piece_consistent_with_meta_(const ObLobPieceInfo &piece,
                                             const ObLobMetaInfo &meta);
  int parse_append_data_(common::ObString &append_data,
                         uint64_t &append_char_len);
  int calculate_total_length_(const uint64_t old_byte_len,
                              const uint64_t old_char_len,
                              const uint64_t append_byte_len,
                              const uint64_t append_char_len,
                              uint64_t &total_byte_len,
                              uint64_t &total_char_len);
  int prepare_remaining_append_data_(const common::ObString &append_data,
                                     const uint64_t append_char_len,
                                     const uint64_t consumed_byte_len,
                                     const uint64_t consumed_char_len,
                                     common::ObString &remaining_append_data);
  int write_new_chunks_(const common::ObLobId &lob_id,
                        const common::ObString &old_inrow_data,
                        const common::ObString &remaining_append_data,
                        const common::ObString &start_seq_id,
                        ObLobWriteStats &write_stats);
  int validate_write_stats_(const ObLobWriteStats &write_stats,
                            const uint64_t expected_byte_len,
                            const uint64_t expected_char_len,
                            const uint64_t old_chunk_count,
                            uint64_t &total_chunk_count);
  int read_old_summary_(const common::ObLobId &lob_id,
                        ObLobMetaInfo &summary,
                        ObLobMetaInfo &last_meta,
                        uint64_t &chunk_count);
  int try_fill_last_piece_(ObLobMetaInfo &last_meta,
                           const common::ObString &append_data,
                           uint64_t &consumed_byte_len,
                           uint64_t &consumed_char_len);
  int write_meta_summary_(const common::ObLobId &lob_id,
                          const bool old_is_outrow,
                          ObLobMetaInfo &old_summary,
                          const uint64_t total_byte_len,
                          const uint64_t total_char_len,
                          const uint64_t chunk_count);
private:
  bool is_inited_;
  ObLobAppendContext *ctx_;
  ObLobReadContext read_ctx_;
  ObLobMetaManager *meta_mgr_;
  ObLobPieceManager *piece_mgr_;
};

class ObLobDeleter
{
public:
  ObLobDeleter();
  ~ObLobDeleter() { reset(); }
  void reset();
  int init(ObLobDeleteContext *ctx, 
           ObLobMetaManager *meta_mgr, 
           ObLobPieceManager *piece_mgr);
  int del();

private:
  bool is_inited_;
  ObLobDeleteContext *ctx_;
  ObLobMetaManager *meta_mgr_;
  ObLobPieceManager *piece_mgr_; 
};

}  // storage
}  // oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_HANDLER_H_
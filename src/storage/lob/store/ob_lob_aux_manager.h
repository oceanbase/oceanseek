/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEABASE_STORAGE_OB_LOB_AUX_MANAGER
#define OCEABASE_STORAGE_OB_LOB_AUX_MANAGER
#include "ob_lob_store_common.h"
#include "lib/task/ob_timer.h"
#include "storage/blocksstable/ob_datum_row_iterator.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/access/ob_dml_param.h"
#include "storage/lob/common/ob_lob_seq.h"
#include "storage/lob/store/ob_lob_persistent_adaptor.h"
#include "storage/lob/access/ob_lob_remote.h"


namespace oceanbase
{
namespace storage
{
class ObPersistentLobAdaptor;
class ObLobQueryIter;
struct ObLobMetaScanResult;
struct ObLobOutTableRedoCtx;
struct ObLobOutTableRedoPayload;

class ObLobMetaManager
{
public:
  ObLobMetaManager() :
    is_inited_(false),
    adaptor_(nullptr)
  {}
  ~ObLobMetaManager() { reset(); }
  int init(ObPersistentLobAdaptor *adaptor);
  // reset to initial state, safe to call before init() or multiple times
  void reset();
  // write one lob meta row
  int insert(ObLobInsertContext &insert_ctx, ObLobMetaInfo &in_row);
  int batch_insert(
      ObLobInsertContext &insert_ctx,
      blocksstable::ObDatumRowIterator &iter,
      const ObLobOutTableRedoCtx *outtable_redo_ctx = nullptr);
  int batch_insert_outtable(
      ObLobInsertContext &insert_ctx,
      ObLobQueryIter *iter,
      const common::ObIArray<ObLobMetaInfo> &metas,
      const bool is_logging);
  int batch_delete(ObLobDeleteContext &delete_ctx, blocksstable::ObDatumRowIterator &iter);
  // scan raw lob meta
  int scan(ObLobReadContext &read_ctx, ObLobMetaTableIter *&iter);
  int revert_scan_iter(ObLobMetaTableIter *iter);
  // specified range erase (named 'erase' because 'delete' is a C++ keyword)
  int del(ObLobDeleteContext &delete_ctx, ObLobMetaInfo &in_row);
  // specified range update
  int update(ObLobUpdateContext &update_ctx, ObLobMetaInfo &old_row, ObLobMetaInfo &new_row);
  int update_outtable(ObLobUpdateContext &update_ctx, ObLobMetaInfo &old_row,
      ObLobMetaInfo &new_row, ObLobQueryIter *iter, const bool is_logging);
  ObPersistentLobAdaptor *get_adaptor() const { return adaptor_; }

  int get_byte_length(ObLobReadContext &read_ctx, uint64_t &byte_length);
  int get_char_length(ObLobReadContext &read_ctx, uint64_t &char_length);
  int get_meta_summary_info(ObLobReadContext &read_ctx, ObLobMetaInfo &summary);
  int get_last_meta_info(ObLobReadContext &read_ctx, ObLobMetaInfo &last_meta);
  int get_table_param(const ObTableParam *&table_param);
  int get_table_dml_param(const ObTableDMLParam *&table_dml_param);
  TO_STRING_KV("[LOB]", "meta mngr");

private:
  static constexpr int64_t REDO_BATCH_SIZE = 8;
  int prepare_outtable_redo_payload_(const ObLobWriteContext &ctx, ObLobQueryIter *iter,
      const ObLobMetaInfo &meta, const uint64_t lob_piece_table_id, const bool is_logging,
      common::ObIAllocator &allocator, ObLobOutTableRedoPayload &payload);

private:
  bool is_inited_;
  // injected lob persistent adaptor, owned by ObLobManager, shared by meta+piece
  ObPersistentLobAdaptor *adaptor_;

  DISALLOW_COPY_AND_ASSIGN(ObLobMetaManager);
};

class ObLobPieceManager
{
public:
  ObLobPieceManager() :
    is_inited_(false),
    adaptor_(nullptr)
  {}
  ~ObLobPieceManager() { reset(); }
  int init(ObPersistentLobAdaptor *adaptor);
  // reset to initial state, safe to call before init() or multiple times
  void reset();

  int batch_insert(ObLobInsertContext &insert_ctx, blocksstable::ObDatumRowIterator &iter);
  int batch_delete(ObLobDeleteContext &delete_ctx, blocksstable::ObDatumRowIterator &iter);
  // === DML (phase1: adaptor returns OB_NOT_SUPPORTED) ===
  int insert(ObLobInsertContext &insert_ctx, ObLobPieceInfo &in_row);
  int del(ObLobDeleteContext &delete_ctx, ObLobPieceInfo &in_row); // named 'erase' because 'delete' is a C++ keyword
  int update(ObLobUpdateContext &update_ctx, ObLobPieceInfo &old_row, ObLobPieceInfo &new_row);
  int scan(ObLobReadContext &read_ctx, ObLobPieceTableIter *&iter);
  int revert_scan_iter(ObLobPieceTableIter *iter);
  int get_piece_info(ObLobReadContext &read_ctx,
                     const common::ObString &seq_id,
                     ObLobPieceInfo &piece_info);
  int get_table_param(const ObTableParam *&table_param);
  int get_table_dml_param(const ObTableDMLParam *&table_dml_param);
  TO_STRING_KV("[LOB]", "piece mngr");
private:
  bool is_inited_;
  // injected lob persistent adaptor, owned by ObLobManager, shared by meta+piece
  ObPersistentLobAdaptor *adaptor_;

  DISALLOW_COPY_AND_ASSIGN(ObLobPieceManager);
};


} // storage
} // namespace 

#endif

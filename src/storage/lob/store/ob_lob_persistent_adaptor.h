/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEABASE_STORAGE_OB_LOB_PERSISTENT_ADAPTOR_
#define OCEABASE_STORAGE_OB_LOB_PERSISTENT_ADAPTOR_
#include "lib/lock/ob_spin_lock.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/blocksstable/ob_datum_row_iterator.h"
#include "storage/access/ob_dml_param.h"
#include "storage/access/ob_table_access_param.h"
#include "ob_lob_store_common.h"
#include "storage/lob/common/ob_lob_context.h"

namespace oceanbase
{
namespace storage
{

class ObPersistentLobAdaptor;
struct ObLobMetaScanResult;

class ObLobAuxTableBaseIterator;
template<typename T>
class ObLobAuxTableIterator;
typedef ObLobAuxTableIterator<ObLobMetaInfo> ObLobMetaTableIter;
typedef ObLobAuxTableIterator<ObLobPieceInfo> ObLobPieceTableIter;


class ObStoreCtxGuard;
// @brief ObPersistentLobAdaptor 负责 LOB Meta 表和 LOB Piece 表的查询、写入、更新和删除。
// 查询方面，支持单独查询 Meta 表中 seq_id = 0 的 Summary 行，也支持扫描 Meta 表和
// Piece 表；扫描迭代器可缓存在 ObLobAccessCtx 中。写入方面，支持单行和批量写入、
// 更新及删除，批量接口需传入 ObDatumRowIterator 的派生迭代器。
// ObPersistentLobAdaptor 不支持远程读写，并负责组装本地读写所需的 DML 参数和上下文。
class ObPersistentLobAdaptor
{
public:
  explicit ObPersistentLobAdaptor(const uint64_t tenant_id):
    tenant_id_(tenant_id),
    allocator_(lib::ObMemAttr(tenant_id, "LobPersist", ObCtxIds::LOB_CTX_ID)),
    lock_(common::ObLatchIds::OB_LOB_PERSISTENT_ADAPTOR_LOCK),
    table_param_inited_(false),
    meta_table_param_(nullptr),
    meta_table_dml_param_(nullptr),
    piece_table_param_(nullptr),
    piece_table_dml_param_(nullptr)
  {}

  ~ObPersistentLobAdaptor();

  void destroy();

public: // read
  int get_meta_summary_info(ObLobReadContext &read_ctx, ObLobMetaInfo &meta_summary_info);
  // Read the tail chunk metadata during append.
  int get_last_meta_info(ObLobReadContext &read_ctx,
                         ObLobMetaInfo &last_meta_info);
  int get_lob_piece_info(ObLobReadContext &read_ctx,
                         const common::ObString &seq_id,
                         ObLobPieceInfo &piece_info);

  int scan_lob_meta(ObLobReadContext &read_ctx,
                    ObLobMetaTableIter *&iter);
  int scan_lob_piece(ObLobReadContext &read_ctx, ObLobPieceTableIter *&iter);
  int scan_with_ctx(const LobAuxTarget target,
                    ObLobReadContext &read_ctx,
                    ObLobAuxTableBaseIterator *&iter);

  int revert_scan_iter(ObLobAuxTableBaseIterator *iter);

public: // write
  int write_lob_meta(ObLobInsertContext &insert_ctx, ObLobMetaInfo &row_info);
  int delete_lob_meta(ObLobDeleteContext &delete_ctx, ObLobMetaInfo &row_info);
  int update_lob_meta(ObLobUpdateContext &update_ctx, ObLobMetaInfo &old_row, ObLobMetaInfo &new_row);

  int write_lob_meta(ObLobInsertContext &insert_ctx, blocksstable::ObDatumRowIterator &row_iter);
  int delete_lob_meta(ObLobDeleteContext &delete_ctx, blocksstable::ObDatumRowIterator &row_iter);
  int update_lob_meta(ObLobUpdateContext &update_ctx, blocksstable::ObDatumRowIterator &row_iter);

  // === piece DML ===
  int write_lob_piece(ObLobInsertContext &insert_ctx, ObLobPieceInfo &row_info);
  int update_lob_piece(ObLobUpdateContext &update_ctx, ObLobPieceInfo &old_row, ObLobPieceInfo &new_row);
  int delete_lob_piece(ObLobDeleteContext &delete_ctx, ObLobPieceInfo &row_info);

  int write_lob_piece(ObLobInsertContext &insert_ctx, blocksstable::ObDatumRowIterator &row_iter);
  int delete_lob_piece(ObLobDeleteContext &delete_ctx, blocksstable::ObDatumRowIterator &row_iter);
  int update_lob_piece(ObLobUpdateContext &update_ctx, blocksstable::ObDatumRowIterator &row_iter);

public:
  // === row <-> datum conversion ===
  static void set_lob_meta_row(blocksstable::ObDatumRow &datum_row,
                               const ObLobMetaInfo &in_row);
  static void set_lob_piece_row(blocksstable::ObDatumRow &datum_row,
                                const ObLobPieceInfo &in_row);

  static void set_lob_row(blocksstable::ObDatumRow &datum_row,
                          const ObLobMetaInfo &meta_info);
  static void set_lob_row(blocksstable::ObDatumRow &datum_row,
                          const ObLobPieceInfo &piece_info);

public:
  // === scan / dml param preparation ===
  int prepare_table_scan_param(const LobAuxTarget target,
                               const ObLobReadContext &read_ctx,
                               const bool is_get,
                               ObTableScanParam &scan_param,
                               ObIAllocator *stmt_allocator,
                               ObIAllocator *scan_allocator);
  
  // @brief prepare table scan param accroding target(maybe meta or piece).
  //        Set schema version through aux tablet handle.
  int prepare_scan_param_schema_version(const LobAuxTarget target,
                                        ObLobReadContext &read_ctx,
                                        ObTableScanParam &scan_param);

  int get_aux_table_param(const LobAuxTarget target, const ObTableParam *&table_param);
  int get_aux_table_dml_param(const LobAuxTarget target, const ObTableDMLParam *&table_param);

private: // write
  // dml param preparation
  // Unified implementation of prepare_lob_{meta,piece}_dml and build_lob_{meta,piece}_table_dml.
  // The corresponding slot on param (dml_base_meta_param_ / dml_base_piece_param_) is selected by
  // target. Both slots are independent (each owns its own table_param_ and
  // store_ctx_guard_) and are lazy-initialized on statement arena, then reused across chunks.
  int prepare_lob_aux_dml(const LobAuxTarget target, ObLobWriteContext &write_ctx);
  int build_lob_aux_table_dml(
      const LobAuxTarget target,
      const ObLobWriteContext &write_ctx,
      ObDMLBaseParam &dml_base_param,
      ObStoreCtxGuard *store_ctx_guard);

  // 为 LOB meta 和 piece 表准备 DML 参数。
  int build_lob_aux_dml_common(const LobAuxTarget target,
                               ObLobWriteContext &write_ctx,
                               ObTabletID &tablet_id,
                               ObDMLBaseParam *&dml,
                               ObSEArray<uint64_t, 8> &column_ids);

  // Unified implementations of write/erase/update_lob_{meta,piece}(ObDatumRowIterator&).
  int do_write_lob_aux(const LobAuxTarget target,
                       ObLobInsertContext &insert_ctx,
                       blocksstable::ObDatumRowIterator &row_iter);
  int do_erase_lob_aux(const LobAuxTarget target,
                       ObLobDeleteContext &delete_ctx,
                       blocksstable::ObDatumRowIterator &row_iter);
  int do_update_lob_aux(const LobAuxTarget target,
                        ObLobUpdateContext &update_ctx,
                        blocksstable::ObDatumRowIterator &row_iter);

  // Apply EN_LOB_META_DML_FAILED error injection on META write path.
  OB_NOINLINE int errsim_meta_dml_failed(ObLobInsertContext &insert_ctx,
                                         blocksstable::ObDatumRowIterator &row_iter,
                                         bool &is_injected);

private: // init
  // 初始化 table param 和 DML param。
  int try_init_table_param();
  int init_table_param();

  // SEArray's second template parameter is only an initial capacity hint. It is
  // decoupled from the column count to avoid a header dependency cycle.
  int init_meta_column_ids(ObSEArray<uint64_t, 7> &meta_column_ids);
  int init_piece_column_ids(ObSEArray<uint64_t, 4> &piece_column_ids);


private: // read
  template<typename T>
  int scan_lob_aux(const LobAuxTarget target,
                   ObLobReadContext &read_ctx,
                   ObLobAuxTableIterator<T> *&iter);

  int build_common_scan_param(
      const LobAuxTarget target,
      const ObLobReadContext &read_ctx,
      const bool is_get,
      uint64_t col_num,
      ObTableScanParam &scan_param,
      ObIAllocator *stmt_allocator,
      ObIAllocator *scan_allocator);
  int get_read_aux_tablet_id(const LobAuxTarget target,
                             const ObLobReadContext &read_ctx,
                             common::ObTabletID &tablet_id) const;
  int get_read_aux_column_cnt(const LobAuxTarget target,
                              uint64_t &column_cnt) const;
  int prepare_table_param(
      const LobAuxTarget target,
      ObTableScanParam &scan_param);

private:

  const uint64_t tenant_id_;
  ObArenaAllocator allocator_;
  mutable ObSpinLock lock_;
  bool table_param_inited_;            // guards lazy init of both meta and piece table params
  ObTableParam *meta_table_param_;
  ObTableDMLParam *meta_table_dml_param_;
  ObTableParam *piece_table_param_;
  ObTableDMLParam *piece_table_dml_param_;

  template <typename T>
  void free_table_param(T *&param)
  {
    if (OB_NOT_NULL(param)) {
      param->reset();
      param->~T();
      allocator_.free(param);
      param = nullptr;
    }
  }

private:
  static const uint64_t LOB_EXPIRE_TIME_US = 3 * 1000 * 1000; // 3s
};


} // storage
} // oceanbase

#endif

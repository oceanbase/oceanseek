/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEABASE_STORAGE_OB_LOB_UTIL_
#define OCEABASE_STORAGE_OB_LOB_UTIL_
#include "common/ob_version_def.h"
#include "lib/charset/ob_charset.h"
#include "lib/ob_define.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "storage/access/ob_dml_param.h"
#include "share/schema/ob_table_schema.h"
#include "common/object/ob_object.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/store/ob_lob_store_common.h"


namespace oceanbase
{
namespace blocksstable
{
class MacroBlockId;
}
namespace storage
{
class ObLSHandle;
class ObTabletHandle;

static constexpr uint64_t MIN_DATA_VERSION_FOR_LOB_DIRECT_LOAD = DATA_VERSION_4_6_2_1;

class ObLobUtil final
{
public:
  static int get_tablet(const ObLobContextBase &ctx,
                        const common::ObTabletID &tablet_id,
                        ObTabletHandle &handle);
  // Resolve the LOB Meta and Piece tablet IDs for the data table.
  static int refresh_lob_tablet_id(ObLobContextBase &ctx);
  // Allocate an ID from the LOB Meta tablet auto-increment sequence.
  static int fetch_tablet_inc_id(ObLobInsertContext &insert_ctx,
                                 uint64_t &tablet_inc_id);
  static bool is_data_version_support_lob_direct_load(const uint64_t data_format_version)
  {
    return data_format_version >= MIN_DATA_VERSION_FOR_LOB_DIRECT_LOAD;
  }
  static int check_data_version_support_lob_direct_load(const uint64_t data_format_version,
                                                        const char *operation);

  // HEAD responses may include double quotes around the ETag.
  static common::ObString split_etag_quotes(const common::ObString &etag);

  // Clamp [query_offset, query_offset + query_len) to [0, length).
  static void adjust_range(const uint64_t length, uint64_t &query_offset, uint64_t &query_len);
  // Convert [char_offset, char_offset + char_length) to [byte_offset, byte_offset + byte_length).
  static void convert_char_range_to_byte_range(const common::ObCollationType &coll_type,
                                               const char *data,
                                               const uint64_t length,
                                               uint64_t &char_offset,
                                               uint64_t &char_length,
                                               uint64_t &byte_offset,
                                               uint64_t &byte_length);

  static int get_lob_aux_table_ids(ObDMLRunningCtx &run_ctx,
                                   uint64_t &lob_meta_table_id,
                                   uint64_t &lob_piece_table_id);
  static int get_lob_piece_table_id(const ObLobWriteContext &ctx, uint64_t &lob_piece_table_id);
  // Check whether a LOB of byte_len bytes should use OutTable storage.
  // OutTable storage requires data version 4.6.3.1 or later.
  static int check_write_outtable(ObLobWriteContext &ctx, const uint64_t byte_len, bool &is_outtable);

private:
  static int get_tablet(const ObLobContextBase &ctx,
                        const common::ObTabletID &tablet_id,
                        ObLSHandle &ls_handle,
                        ObTabletHandle &handle);
  static bool check_lob_tablet_id(const common::ObTabletID &data_tablet_id,
                                  const common::ObTabletID &lob_meta_tablet_id,
                                  const common::ObTabletID &lob_piece_tablet_id);
  static int fetch_tablet_inc_id_for_split_src(const ObLobInsertContext &insert_ctx,
                                               const common::ObTabletID &lob_tablet_id,
                                               uint64_t &tablet_inc_id);
};

class ObLobCharsetUtil
{
public:
  static ObCollationType get_collation_type(ObObjType type, ObCollationType ori_coll_type);
  static void transform_query_result_charset(
      const common::ObCollationType &coll_type,
      const char *data,
      uint32_t len,
      uint32_t &byte_len,
      uint32_t &byte_st);
};

class ObLobWriteIter;

class ObInsertLobColumnHelper final
{
public:
  static const uint64_t LOB_TX_TIMEOUT = 86400000000; // 1 day
  static const uint64_t LOB_ACCESS_TX_TIMEOUT = 60000000; // 60s
  static const uint64_t LOB_ALLOCATOR_RESET_CYCLE = 128;
public:
  static int start_trans(const share::ObLSID &ls_id,
                         const bool is_for_read,
                         const int64_t timeout_ts,
                         transaction::ObTxDesc *&tx_desc);
  static int end_trans(transaction::ObTxDesc *tx_desc,
                       const bool is_rollback,
                       const int64_t timeout_ts);
  static int insert_lob_column(ObIAllocator &allocator,
                               const share::ObLSID ls_id,
                               const common::ObTabletID tablet_id,
                               const ObObjType &obj_type,
                               const ObCollationType &cs_type,
                               const ObLobStorageParam &lob_storage_param,
                               blocksstable::ObStorageDatum &datum,
                               const int64_t timeout_ts,
                               const uint64_t src_tenant_id);

  // lob_allocator is mainly used for outrow lob read and write memory allocation,
  // that can be released after lob inset to avoid hold too many memory 
  // and res_allocator is mainly used to alloc lob result datum memory in main table
  // should call iter.close outter
  static int insert_lob_column(ObIAllocator &res_allocator,
                               ObIAllocator &lob_allocator,
                               transaction::ObTxDesc *tx_desc,
                               share::ObTabletCacheInterval &lob_id_geneator,
                               const share::ObLSID ls_id,
                               const common::ObTabletID tablet_id,
                               const common::ObTabletID lob_meta_tablet_id,
                               const common::ObTabletID lob_piece_tablet_id,
                               const int64_t lob_col_idx,
                               const ObObjType &obj_type,
                               const ObCollationType collation_type,
                               const ObLobStorageParam &lob_storage_param,
                               blocksstable::ObStorageDatum &datum,
                               const int64_t timeout_ts,
                               const uint64_t src_tenant_id,
                               const transaction::ObTxSEQ &parent_seq_no,
                               transaction::ObTxSEQ &target_lob_seq_no,
                               ObLobWriteIter &iter,
                               bool &is_target_outrow);
};

struct ObLobDmlTask
{
  ObLobDmlTask():
    op_(ObLobDmlOp::INVALID),
    src_loc_(),
    dest_loc_(),
    old_loc_(),
    col_idx_(OB_INVALID_INDEX),
    row_idx_(OB_INVALID_INDEX)
  {
  }

  // 追加写的两个条件，在Update中且目标lob为Append_DELTA_LOB
  bool is_append() const
  { return ObLobDmlOp::UPDATE == op_ && src_loc_.is_append_delta_lob(); }
  // 只有右值为 append_delta_lob 时进行 append，否则为正常 Update。
  bool is_update() const
  { return ObLobDmlOp::UPDATE == op_ && !src_loc_.is_append_delta_lob(); }
  bool is_valid() const
  {
    bool valid = false;
    if (OB_INVALID_INDEX == col_idx_ || OB_INVALID_INDEX == row_idx_) {
      valid = false;
    } else if (ObLobDmlOp::INSERT == op_) {
      valid = need_insert_outrow();
    } else if (ObLobDmlOp::UPDATE == op_) {
      valid = is_append()
          ? need_insert_outrow()
          : need_insert_outrow() || need_delete_old_outrow();
    } else if (ObLobDmlOp::DELETE == op_) {
      valid = need_delete_old_outrow();
    }
    return valid;
  }

  bool need_insert_outrow() const { return dest_loc_.is_outline_disk_lob_locator(); }
  bool need_delete_old_outrow() const { return old_loc_.is_outline_disk_lob_locator(); }

  int from_ctx(const ObLobInsertContext &ctx, const int16_t row_idx, const int16_t col_idx);
  int from_ctx(const ObLobUpdateContext &ctx, const int16_t row_idx, const int16_t col_idx);
  int from_ctx(const ObLobAppendContext &ctx, const int16_t row_idx, const int16_t col_idx);

  ObLobDmlOp op_;
  ObLobLocatorV2 src_loc_;
  ObLobLocatorV2 dest_loc_;
  // old outrow value, only set for outrow->outrow update, used to delete old lob data in update_after
  ObLobLocatorV2 old_loc_;
  int16_t col_idx_;
  int16_t row_idx_;
  TO_STRING_KV(K_(op), K_(col_idx), K_(row_idx), K_(src_loc), K_(dest_loc), K_(old_loc));
};

struct ObLobExtInfoLogTask final
{
  ObLobExtInfoLogTask() { reset(); }
  ObLobExtInfoLogTask(
      const int16_t row_idx,
      const int16_t col_idx,
      const transaction::ObTxSEQ &lob_operation_seq_no)
    : row_idx_(row_idx),
      col_idx_(col_idx),
      lob_operation_seq_no_(lob_operation_seq_no)
  {}
  ~ObLobExtInfoLogTask() { reset(); }

  void reset()
  {
    row_idx_ = OB_INVALID_INDEX;
    col_idx_ = OB_INVALID_INDEX;
    lob_operation_seq_no_.reset();
  }
  bool is_valid() const
  {
    return (OB_INVALID_INDEX != row_idx_)
        && (OB_INVALID_INDEX != col_idx_)
        && lob_operation_seq_no_.is_valid();
  }

  TO_STRING_KV(K_(row_idx), K_(col_idx), K_(lob_operation_seq_no));

  int16_t row_idx_;
  int16_t col_idx_;
  transaction::ObTxSEQ lob_operation_seq_no_;
};

struct ObLobTabletDmlCtx
{
  ObLobTabletDmlCtx()
    : insert_task_array_(),
      ext_info_log_task_array_(),
      lob_meta_table_id_(OB_INVALID_ID),
      lob_piece_table_id_(OB_INVALID_ID)
  {
    reset();
    insert_task_array_.set_attr(ObMemAttr(MTL_ID(), "LobDml"));
    ext_info_log_task_array_.set_attr(ObMemAttr(MTL_ID(), "LobExtInfo"));
  }

  ~ObLobTabletDmlCtx() { reset(); }

  void reset()
  {
    insert_task_array_.reset();
    ext_info_log_task_array_.reset();
    lob_meta_table_id_ = OB_INVALID_ID;
    lob_piece_table_id_ = OB_INVALID_ID;
  }

  void reuse()
  {
    insert_task_array_.reuse();
    ext_info_log_task_array_.reuse();
  }

  bool insert_task_empty() const { return insert_task_array_.empty(); }

  int64_t insert_task_count() const { return insert_task_array_.count(); }

  int push_insert_task(const ObLobDmlTask &task)
  {
    return insert_task_array_.push_back(task);
  }

  bool ext_info_log_task_empty() const { return ext_info_log_task_array_.empty(); }

  int64_t ext_info_log_task_count() const { return ext_info_log_task_array_.count(); }

  int push_ext_info_log_task(const ObLobExtInfoLogTask &task)
  {
    return ext_info_log_task_array_.push_back(task);
  }

  TO_STRING_KV(K_(insert_task_array), K_(ext_info_log_task_array),
      K_(lob_meta_table_id), K_(lob_piece_table_id));
  ObArray<ObLobDmlTask> insert_task_array_;
  ObArray<ObLobExtInfoLogTask> ext_info_log_task_array_;
  uint64_t lob_meta_table_id_;
  uint64_t lob_piece_table_id_;
};

} // storage
} // oceanbase

#endif

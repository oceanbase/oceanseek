/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_OB_LOB_MANAGER_
#define OCEABASE_STORAGE_OB_LOB_MANAGER_
#include "lib/lock/ob_spin_lock.h"
#include "lib/task/ob_timer.h"
#include "lib/hash/ob_cuckoo_hashmap.h"
#include "object/ob_object.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/store/ob_lob_aux_manager.h"
#include "storage/lob/store/ob_lob_meta.h"
#include "storage/lob/access/ob_lob_iterator.h"
#include "storage/ob_storage_rpc.h"
#include "share/throttle/ob_share_resource_throttle_tool.h"
#include "share/lob/ob_lob_ext_info_log_allocator.h"

namespace oceanbase
{
namespace storage
{

class ObLobDiskLocatorBuilder;

class ObLobManager
{
public:
  static const int64_t LOB_OUTROW_FULL_SIZE = ObLobLocatorV2::DISK_LOB_OUTROW_FULL_SIZE;
  static const uint64_t LOB_READ_BUFFER_LEN = 1024L*1024L; // 1M
  // 该限制针对累计的行外 LOB 值，而非单个 SQL 数据。
  // 上限比 4 TiB 少一个字节，保证所有有效字节偏移均严格小于 4 TiB。
  static constexpr uint64_t LOB_WRITEAPPEND_MAX_BYTE_LEN = (4ULL << 40) - 1;
  static const ObLobCommon ZERO_LOB; // static empty lob for zero val
private:
  explicit ObLobManager(const uint64_t tenant_id)
    : tenant_id_(tenant_id),
      is_inited_(false),
      allocator_(tenant_id),
      adaptor_(tenant_id),
      meta_manager_(),
      piece_manager_()
  {}
public:
  ~ObLobManager() { destroy(); }
  static int mtl_new(ObLobManager *&m);
  static void mtl_destroy(ObLobManager *&m);
  // MTL 
  static int mtl_init(ObLobManager *&m);
  int init();
  int start();
  int stop();
  void wait();
  void destroy();

  // Only use for default lob col val
  static int fill_lob_header(ObIAllocator &allocator, ObString &data, ObString &out);
  static int fill_lob_header(ObIAllocator &allocator, ObStorageDatum &datum);
  static int fill_lob_header(ObIAllocator &allocator,
                             const ObIArray<share::schema::ObColDesc> &column_ids,
                             blocksstable::ObDatumRow &datum_row);
  static int normalize_to_disk_inrow_locator(common::ObObj &obj);
  int build_lob_read_context(ObLobReadContext &read_ctx,
                             ObIAllocator &allocator,
                             ObCollationType coll_type,
                             uint64_t offset,
                             uint64_t len,
                             int64_t timeout,
                             ObLobLocatorV2 &lob,
                             transaction::ObTxDesc *tx_desc,
                             ObLobAccessCtx *lob_access_ctx);
  int build_lob_write_context(ObLobWriteContext &write_ctx,
                              ObIAllocator &allocator,
                              ObCollationType coll_type,
                              uint64_t offset,
                              uint64_t len,
                              int64_t timeout,
                              ObLobLocatorV2 &lob);
    
  int get_table_param(const ObTableParam *&table_param);
  int get_table_dml_param(const ObTableDMLParam *&table_dml_param);
  common::ObIAllocator &get_ext_info_log_allocator() { return ext_info_log_allocator_; }
  share::LobExtInfoLogThrottleTool &get_ext_info_log_throttle_tool() { return throttle_tool_; }
  inline bool can_write_inrow(uint64_t len, int64_t inrow_threshold) { return len <= inrow_threshold; }
public:
  int query(ObLobLocatorV2 &loc, ObString &data);
  int query(ObLobReadContext &read_ctx, ObString &data);
  int query(ObLobReadContext &read_ctx, ObLobQueryIter *&iter);
  // TODO fengyun 需要改造, 定义为 query 不合适
  // 本质是从 str 中构建 iter
  int query(ObString &str, ObLobQueryIter *&iter);

  int insert_before(ObLobInsertContext &param, bool &outrow);
  int update_before(ObLobUpdateContext &param, bool &outrow);
  int append_before(ObLobAppendContext &param, bool &outrow);
  int insert(ObLobInsertContext &param);
  int append(ObLobAppendContext &param);

  int del(ObLobDeleteContext &del_ctx);

  static int get_byte_length(ObLobLocatorV2 &loc, uint64_t &byte_len);
  int get_char_length(ObLobReadContext &read_ctx, uint64_t &char_len);
  int get_byte_length(ObLobReadContext &read_ctx, uint64_t &byte_len);
  int get_lob_summary_info(ObLobReadContext &read_ctx, ObLobMetaInfo &lob_info);

public:
  int compare(ObLobLocatorV2 &lob_left,
              ObLobLocatorV2 &lob_right,
              ObLobCompareParams &cmp_params,
              int64_t &result);
  int compare(ObLobReadContext &param_left,
              ObLobReadContext &param_right,
              int64_t &result);
  int equal(ObLobLocatorV2 &lob_left,
            ObLobLocatorV2 &lob_right,
            ObLobCompareParams &cmp_params,
            bool &result);

public:
  ObLobMetaManager &get_meta_manager() { return meta_manager_; }
  ObLobPieceManager &get_piece_manager() { return piece_manager_; }
private:
  // @brief build outrow dest locator. reuse_lob_id valid: reuse it (outrow->outrow update);
  //  invalid: alloc a new lob id (insert or null/inrow->outrow update).
  int prepare_outrow_locator_(ObLobInsertContext &insert_ctx, const ObLobId &reuse_lob_id);
  int prepare_lob_id_(ObLobInsertContext &insert_ctx,
                      ObLobDiskLocatorBuilder &locator_builder, 
                      const ObLobId &reuse_lob_id);
  // @brief alloc lob id for outrow lob, two cases:
  //  case 1: heap table (without pk), the lob_id contains column_idx and rowkey
  //  case 2: the table with pk, the lob_id contains column_idx, table_id (the lob meta), auto_inc_id
  int alloc_lob_id_(ObLobInsertContext &insert_ctx, ObLobId &lob_id);
  // @brief get one ObTxSEQ, used for write the row in meta table where seq_id = 0
  int prepare_seq_no_(ObLobInsertContext &insert_ctx, ObLobDiskLocatorBuilder &locator_builder);

private:
  const uint64_t tenant_id_;
  bool is_inited_;
  common::ObFIFOAllocator allocator_;
  // single persistent adaptor shared by lob meta and lob piece
  ObPersistentLobAdaptor adaptor_;
  ObLobMetaManager meta_manager_;
  ObLobPieceManager piece_manager_;
  share::LobExtInfoLogThrottleTool throttle_tool_;
  share::ObLobExtInfoLogAllocator ext_info_log_allocator_;
};

} // storage
} // oceanbase

#endif

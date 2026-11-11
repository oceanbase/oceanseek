/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_HELPER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_HELPER_H_

#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace storage
{

const char *const CLUSTER_DIR_STR = "cluster";
const char *const SERVER_DIR_STR = "server";
const char *const TENANT_DIR_STR = "tenant";
const char *const CKPT_STR = "ckpt";
const char *const SLOG_STR = "slog";
const char *const LS_DIR_STR = "ls";
const char *const TABLET_DIR_STR = "tablet";
const char *const TABLET_IDS_DIR_STR = "tablet_ids";
const char *const TABLET_DATA_DIR_STR = "tablet_data";
const char *const TABLET_META_DIR_STR = "tablet_meta";
const char *const TX_CTX_TABLET_DIR_STR = "TX_CTX";
const char *const TX_DATA_TABLET_DIR_STR = "TX_DATA";
const char *const TX_LOCK_TABLET_DIR_STR = "TX_LOCK";
const char *const REORG_INFO_TABLET_DIR_STR = "REORG_INFO";
const char *const MDS_TABLET_DIR_STR = "MDS_DATA";
const char *const OBJECT_TYPE_STR = ".T";
const char *const DEFAULT_TMP_STR = ".tmp.";
const char *const DEFAULT_DELETED_STR = ".deleted";
const char *const MINI_DIR_STR = "mini";
const char *const MINOR_DIR_STR = "minor";
const char *const MAJOR_DIR_STR = "major";
const char *const INC_MAJOR_DIR_STR = "inc_major";
const char *const COLUMN_GROUP_STR = "cg";
const char *const DATA_MACRO_DIR_STR = "data";
const char *const TMP_DATA_DIR_STR = "tmp_data";
const char *const SHARED_MINI_MACRO_CACHE_DIR_STR = "shared_mini_macro_cache";
const char *const SHARED_MINOR_MACRO_CACHE_DIR_STR = "shared_minor_macro_cache";
const char *const SHARED_MAJOR_MACRO_CACHE_DIR_STR = "shared_major_macro_cache";
const char *const MAJOR_DATA_DIR_STR = "shared_major_macro_cache";
const char *const INC_MAJOR_DATA_DIR_STR = "shared_inc_major_macro_cache";
const char *const SHARED_TABLET_SUB_META_CACHE_DIR_STR = "shared_tablet_sub_meta_cache";
const char *const COMPACTION_DIR_STR = "compaction";
const char *const COMPACTOR_DIR_STR = "compactor";
const char *const SCHEDULER_DIR_STR = "scheduler";
const char *const SHARED_TABLET_SSTABLE_DIR_STR = "sstable";
const char *const IO_DETECT_FILE_NAME = "io_detect_file";
const char *const EXTERNAL_TABLE_FILE_DIR_STR = "external_table_file";
const char *const EXTERNAL_LOB_CACHE_DIR_STR = "external_lob_cache";
const char *const MACRO_CACHE_CKPT_DIR_STR = "macro_cache_ckpt";
const char *const CKM_ERROR_DIR_STR = "checksum_error_macro";
const char *const OP_ID_DIR_STR = "op";
const char *const PREWARM_INFO_DIR_STR = "prewarm_info";
const char *const SVR_KEY_STR = "svr";
const char *const SEQ_KEY_STR = "seq";
const char *const OP_KEY_STR = "op";
const char *const TABLET_KEY_STR = "t";
const char *const REORG_KEY_STR = "rs"; // reorganization_scn
const char *const CG_KEY_STR = "cg";
const char *const SEG_KEY_STR = "seg";
const char *const VER_KEY_STR = "ver";
const char *const TFR_KEY_STR = "tsfr";
const char *const SCN_KEY_STR = "scn";
const char *const LEN_KEY_STR = "len";
const char *const IDX_KEY_STR = "idx";
const char *const META_DIR_STR = "meta";
const char *const LOCK_DIR_STR = "lock";
const char *const MARK_SWEEP_GC_DIAG_DIR_STR = "mark_sweep_gc_diag";
const char *const ROUND_DIR_STR = "round";
const char *const BKT_KEY_STR = "bkt";
const int64_t OBJ_TIERED_METADATA_SEQ_WIDTH = 10L;

enum ObMacroType : uint8_t
{
  DATA_MACRO = 0,
  META_MACRO = 1,
};

static const char *macro_type_strs[] = {DATA_MACRO_DIR_STR, META_DIR_STR};

static const char *get_macro_type_str(const ObMacroType type)
{
  return macro_type_strs[static_cast<int32_t>(type)];
}

class ObPathContext
{
public:
  ObPathContext();
  ~ObPathContext();
  ObPathContext& operator =(const ObPathContext &other);
  bool is_valid() const;
  void reset();
  int set_file_ctx(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id,
                   const bool is_local_cache, const uint64_t server_id, const int64_t ls_id = 0);
  int set_logical_delete_ctx(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id,
                             const uint64_t server_id, const int64_t ls_id = 0);  // set for .deleted file
  int set_atomic_write_ctx(const blocksstable::MacroBlockId &file_id, const int64_t ls_epoch_id,
                           const uint64_t write_seq, const uint64_t server_id,
                           const int64_t ls_id = 0);  // set for .tmp.seq file
  OB_INLINE const char *get_path() const { return path_; }
  TO_STRING_KV(K_(path), K_(file_id), K_(ls_id), K_(ls_epoch_id), K_(server_id), K_(is_local_cache),
               K_(is_atomic_write), K_(write_seq), K_(is_logical_delete));
private:
  bool is_macro_block_id_valid() const;
  int to_path();
private:
  char path_[common::MAX_PATH_SIZE];
  blocksstable::MacroBlockId file_id_;
  int64_t ls_id_;  // private tablet meta keeps the LS directory namespace outside MacroBlockId
  int64_t ls_epoch_id_;  // meta_file need ls_epoch_id, because meta_file is stored in ls_dir
  uint64_t server_id_; // private object writer namespace
  bool is_local_cache_;  // store in local is true, store in remote is false
  bool is_atomic_write_;  // if true, concat .tmp.seq to path
  uint64_t write_seq_;  // used for atomic_write
  bool is_logical_delete_;  // if true, concat .deleted to path
};

class ObFileHelper
{
public:
  static int parse_file_name(const char *file_name, int64_t &file_id); // only used for file_name is a number
  static int parse_tmp_file_name(const char *file_name, int64_t &file_id); // only used for tmp_file_name is a number or number.deleted
  static int tmpfile_path_to_macro_id(const char *dir_path, const char *file_name, blocksstable::MacroBlockId &file_id); // convert tmpfile path to macro id
  static int get_file_parent_dir(char *path, const int64_t length,
                                 const blocksstable::MacroBlockId &file_id,
                                 const int64_t ls_epoch_id = 0, const int64_t ls_id = 0);
  static int create_file_parent_dir(const blocksstable::MacroBlockId &file_id,
                                    const int64_t ls_id, const int64_t ls_epoch_id);
  static int convert_path_to_macro_id(const char *path, blocksstable::MacroBlockId &macro_id);
  static const char *get_ls_inner_tablet_name(const int64_t tablet_id);
  static int get_ls_inner_tablet_id(const char *str, int64_t &tablet_id);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_FILE_HELPER_H_ */

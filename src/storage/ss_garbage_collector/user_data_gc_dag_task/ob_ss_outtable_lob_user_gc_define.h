/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_USER_DATA_GC_DAG_TASK_OB_SS_OUTTABLE_LOB_USER_GC_DEFINE_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_USER_DATA_GC_DAG_TASK_OB_SS_OUTTABLE_LOB_USER_GC_DEFINE_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/garbage_collector/ob_mark_sweep_tmp_file_mgr.h"
#include "storage/lob/store/ob_outtable_lob_control_operator.h"
#include "storage/ob_parallel_external_sort.h"

namespace oceanbase
{
namespace storage
{

struct ObSSOuttableLobUserGCRoundParam final
{
  ObSSOuttableLobUserGCRoundParam();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(round_id), K_(safe_tx_scn));

  uint64_t tenant_id_;
  int64_t round_id_;
  share::SCN safe_tx_scn_;
};

struct ObSSOuttableLobCtrlCandidate final
{
  ObSSOuttableLobCtrlCandidate();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(macro_id), K_(hash), K_(mark_row_scn));
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObSSOuttableLobCtrlCandidate &src, char *buf, int64_t len, int64_t &pos);

  // Ctrl 主键由 macro_id 和 hash 共同组成。
  blocksstable::MacroBlockId macro_id_;
  uint64_t hash_;
  int64_t mark_row_scn_;
  OB_UNIS_VERSION(1);
};

struct ObSSLobCtrlGCScanCompare final
{
  ObSSLobCtrlGCScanCompare();
  bool operator()(const ObSSOuttableLobCtrlCandidate *left, const ObSSOuttableLobCtrlCandidate *right);

  int result_code_;
};

typedef ObExternalSort<ObSSOuttableLobCtrlCandidate, ObSSLobCtrlGCScanCompare> ObSSLobCtrlGCScanSorter;

struct ObSSLobCtrlGCScanFile final
{
  ObSSLobCtrlGCScanFile();
  ~ObSSLobCtrlGCScanFile();
  int assign(const ObSSLobCtrlGCScanFile &other);
  void reset();
  bool is_valid() const;
  int64_t get_fd() const;
  int64_t get_dir_id() const;
  TO_STRING_KV(K_(file_handle), K_(sample_item), K_(item_count));

  ObMarkSweepTmpFileHandle file_handle_;
  ObSSOuttableLobCtrlCandidate sample_item_;
  int64_t item_count_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSLobCtrlGCScanFile);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_USER_DATA_GC_DAG_TASK_OB_SS_OUTTABLE_LOB_USER_GC_DEFINE_H_

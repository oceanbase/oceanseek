/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_GC_DIAGNOSE_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_GC_DIAGNOSE_H_

#include "lib/container/ob_array.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/garbage_collector/ob_mark_sweep_pipeline.h"
#include "storage/ss_garbage_collector/ob_ss_mark_sweep_diag_mgr.h"

namespace oceanbase
{
namespace observer
{

// Virtual table __all_virtual_shared_gc_diagnose.
//
// Surfaces the SSMarkSweepDiag (mark-sweep GC diagnosis) records that a GC round writes
// to object storage for GC troubleshooting. A query MUST specify
// round_id (point predicate), otherwise it errors. tenant_id rule: under a user/meta
// tenant only the current tenant is visible (any specified tenant_id is ignored); under
// the sys tenant tenant_id is optional -- when specified it targets that tenant, when
// omitted it targets the sys tenant itself. group/type/ls_id/tablet_id/reorganization_scn
// are optional point filters.
//
// Diag files live at:
//   {root}/cluster_{id}/tenant_{id}/mark_sweep_gc_diag/round{id}/{group}/{type}/bkt{bucket}.T32
// Study uses kept/study_results; other type directories stay unchanged.
// They are read directly from object storage by explicit tenant_id (NOT MTL_ID()), so a
// sys connection can read a tenant whose Unit is not on this server. Each (group, type)
// directory holds numerically-named bucket files; each file holds one record per tablet,
// and a record carries either the tablet's macro ids (type=macros) or its meta_scns
// (type=tablet_meta_scns). One virtual row is emitted per item:
//   type=macros           -> value = MacroBlockId.to_string
//   type=tablet_meta_scns -> value = meta_scn
//   type=study_results   -> VALUE holds source-node addr and two raw watermarks; group is kept only.
//
// Rows are produced lazily via a 5-level cursor (group -> type -> bucket -> file batch ->
// record -> item), and bucket files are read in <=2MB batches to bound memory.
class ObAllVirtualSharedGCDiagnose : public common::ObVirtualTableScannerIterator
{
public:
  ObAllVirtualSharedGCDiagnose();
  virtual ~ObAllVirtualSharedGCDiagnose();
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;

private:
  enum class DiagGroupType : uint8_t
  {
    DEL = 0,
    KEPT,
    LOB_USER_GC,
    MAX
  };

  enum COLUMN_ID
  {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    ROUND_ID,
    GROUP,
    TYPE,
    LS_ID,
    TABLET_ID,
    REORGANIZATION_SCN,
    VALUE,
  };

  void reset_state_();
  // extract and validate predicates from key_ranges_ on first call.
  int extract_predicates_();
  int handle_key_range_(common::ObNewRange &key_range);
  // list numeric bucket file names under one (group, type) dir for target_tenant_id_.
  int list_buckets_(const storage::ObMarkSweepGroupType group,
                    const storage::ObSSMarkSweepDiagFileType type,
                    common::ObIArray<int64_t> &buckets);
  // drive the 5-level cursor to the next emittable row; has_row=false at the end.
  int advance_cursor_to_next_row_(bool &has_row);
  // advance group/type/bucket and open the next bucket file reader; opened=false at end.
  int open_next_file_(bool &opened);
  // load the next batch of records from the current file; has_batch=false when drained.
  int next_record_batch_(bool &has_batch);
  bool pass_optional_filters_(const storage::ObSSGCTablet *tablet) const;
  int construct_cur_value_(common::ObString &value);
  int fill_cells_(const storage::ObSSGCTablet *tablet, const common::ObString &value);
  static DiagGroupType get_diag_group_(const storage::ObMarkSweepGroupType group,
                                      const storage::ObSSMarkSweepDiagFileType type);
  static const char *group_to_str_(const storage::ObMarkSweepGroupType group,
                                  const storage::ObSSMarkSweepDiagFileType type);
  static const char *type_to_str_(const storage::ObSSMarkSweepDiagFileType type);

private:
  // value column width is varchar(1024); a MacroBlockId string / meta_scn fits well within.
  static const int64_t MAX_VALUE_LEN = 1024;
  static const int64_t ROWKEY_COL_CNT = 7;
  // ===== predicates / target =====
  bool predicates_extracted_;
  int64_t round_id_;
  uint64_t target_tenant_id_;
  bool has_group_filter_;
  DiagGroupType group_filter_;
  bool has_type_filter_;
  storage::ObSSMarkSweepDiagFileType type_filter_;
  bool has_ls_id_filter_;
  int64_t ls_id_filter_;
  bool has_tablet_id_filter_;
  int64_t tablet_id_filter_;
  bool has_reorg_scn_filter_;
  uint64_t reorg_scn_filter_;

  // ===== 5-level cursor state =====
  int64_t group_idx_;   // index into GROUPS_
  int64_t type_idx_;     // index into TYPES_
  common::ObArray<int64_t> buckets_; // bucket file names under the current (group, type)
  int64_t bucket_idx_;   // index into buckets_
  bool is_buckets_listed_;  // whether buckets_ has been listed for the current (group, type)
  storage::ObSSMarkSweepDiagFileReader reader_; // current bucket file reader
  bool is_reader_opened_;
  // current batch of raw record payloads and the cursor within it.
  common::ObArray<common::ObString> cur_records_;
  int64_t record_idx_;   // index into cur_records_
  bool is_cur_record_decoded_;
  storage::ObSSMarkSweepDiagTabletMacros cur_macros_;     // decoded when current type=macros
  storage::ObSSMarkSweepTabletMetaScns cur_meta_scns_;    // decoded when current type=tablet_meta_scns
  storage::ObSSLobMetaGCDiagRows cur_lob_meta_rows_;
  storage::ObSSLobCtrlGCDiagRows cur_lob_ctrl_rows_;
  storage::ObSSMarkSweepDiagStudyResults cur_study_results_; // Raw per-node watermarks of the current record
  int64_t item_idx_;     // index into the current record's items

  // ===== allocators / buffers =====
  common::ObArenaAllocator batch_allocator_; // owns cur_records_ payloads; reset per batch
  common::ObArenaAllocator record_allocator_; // owns decoded record item arrays; reset per record
  char value_buf_[MAX_VALUE_LEN];
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualSharedGCDiagnose);
};

} // namespace observer
} // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_SHARED_GC_DIAGNOSE_H_ */

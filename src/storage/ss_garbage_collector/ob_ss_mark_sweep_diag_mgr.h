/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_MARK_SWEEP_DIAG_MGR_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_MARK_SWEEP_DIAG_MGR_H_

#include "common/storage/ob_io_device.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/backup/ob_backup_struct.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/ss_garbage_collector/ob_ss_gc_define.h"
#include "storage/garbage_collector/ob_mark_sweep_pipeline.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_outtable_lob_user_gc_define.h"

namespace oceanbase
{
namespace storage
{

struct ObSSGCStudyDiagServerResult
{
  ObSSGCStudyDiagServerResult() : ls_id_(), server_(), tablet_results_() {}
  TO_STRING_KV(K_(ls_id), K_(server), "tablet_count", tablet_results_.count());
  share::ObLSID ls_id_;
  common::ObAddr server_;
  ObSSGCTabletStudyResultArray tablet_results_;
};

class ObSSGCStudyDiagInfo
{
public:
  explicit ObSSGCStudyDiagInfo(const uint64_t tenant_id);
  ~ObSSGCStudyDiagInfo();
  void reset();
  void reset_ls_study_results(const share::ObLSID &ls_id);
  int save_study_result(
      const common::ObAddr &server,
      const ObSSGCTabletWithDelScnArray &batch_tablets,
      const common::ObIArray<ObSSGCTabletStudyResult> &study_results);
  int get_first_fail_ret() const { return first_fail_ret_; }
  const char *get_first_fail_stage() const { return first_fail_stage_; }
  void mark_diag_failed(
      const int ret_code,
      const char *stage);
  const common::ObIArray<ObSSGCStudyDiagServerResult *> &get_study_results() const
  { return study_results_; }

  // Thread binding: the study call chain runs synchronously on the gen-task timer thread, so the
  // collector is bound per-thread instead of threading a diag_info* through every signature.
  // ObSSGCStudyDiagGuard owns the binding. If study later becomes cross-thread, replace this
  // thread-local scheme; do not keep relying on the current binding.
  class Scope final
  {
  public:
    // nullptr (diagnose disabled) does not bind; ctor/dtor are no-ops.
    explicit Scope(ObSSGCStudyDiagInfo *diag_info);
    ~Scope();
  private:
    ObSSGCStudyDiagInfo *prev_diag_info_;
    bool is_bound_;
    DISALLOW_COPY_AND_ASSIGN(Scope);
  };
  static ObSSGCStudyDiagInfo *get_cur_diag_info();
  static void try_reset_ls_study_results(const share::ObLSID &ls_id);
  static void try_save_study_result(
      const common::ObAddr &server,
      const ObSSGCTabletWithDelScnArray &batch_tablets,
      const common::ObIArray<ObSSGCTabletStudyResult> &study_results);

private:
  void clear_study_results_();
  uint64_t tenant_id_;
  int first_fail_ret_;
  const char *first_fail_stage_;
  common::ObArray<ObSSGCStudyDiagServerResult *> study_results_;
  DISALLOW_COPY_AND_ASSIGN(ObSSGCStudyDiagInfo);
};

// Diagnose file types under one round/group directory.
enum class ObSSMarkSweepDiagFileType : uint8_t
{
  MACROS = 0,
  TABLET_META_SCNS = 1,
  STUDY_RESULTS = 2,
  LOB_META_KEPT = 3,
  LOB_CTRL_CANDIDATE = 4,
  LOB_CTRL_DIFF = 5,
  MAX
};

// ====================================================================================
// File-tail layout. Each bucket file is:
//   Record_0 ... Record_{n-1}  Index(variable-length)  Tailer(fixed-length)
// One record == one tablet. The variable-length Index right after the data region
// carries each record's length; the fixed-length Tailer at the very end points to the
// Index start. The reader does: pread fixed Tailer -> Index -> data region. The file is
// only complete()'d on success, so it is always whole.
// ====================================================================================

// Variable-length index: record_count + rec_length[record_count]. Sits right after the
// data region; record offsets are the prefix sum of rec_lengths_ (data region from 0).
class ObSSMarkSweepDiagIndex final
{
  OB_UNIS_VERSION(1);
public:
  ObSSMarkSweepDiagIndex();
  ~ObSSMarkSweepDiagIndex() { reset(); }
  void reset() { record_lens_.reset(); }
  void set_attr(const common::ObMemAttr &attr) { record_lens_.set_attr(attr); }
  int add_record_len(const int64_t len) { return record_lens_.push_back(len); }
  int64_t count() const { return record_lens_.count(); }
  const common::ObIArray<int64_t> &get_record_lens() const { return record_lens_; }

  TO_STRING_KV("rec_cnt", record_lens_.count());

private:
  common::ObSArray<int64_t> record_lens_;
};

// Fixed-length tailer at the very end of the file: magic, version, index_offset.
// index_size is NOT stored: it is derivable as file_len - get_serialize_size() - index_offset.
// All fields are encoded with fixed-width serialization::encode_i64, so get_serialize_size()
// returns a constant (independent of field values); the reader preads it off any instance.
class ObSSMarkSweepDiagTailer final
{
public:
  static const int64_t MAGIC = 0x4743444941474C31; // "GCDIAGL1"
  static const int64_t VERSION = 1;

  ObSSMarkSweepDiagTailer() : magic_(MAGIC), version_(VERSION), index_offset_(0) {}
  explicit ObSSMarkSweepDiagTailer(const int64_t index_offset)
    : magic_(MAGIC), version_(VERSION), index_offset_(index_offset) {}
  ~ObSSMarkSweepDiagTailer() { reset(); }
  void reset() { magic_ = MAGIC; version_ = VERSION; index_offset_ = 0; }
  bool is_valid() const { return (MAGIC == magic_) && (VERSION == version_) && (index_offset_ >= 0); }
  int64_t get_index_offset() const { return index_offset_; }

  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(magic), K_(version), K_(index_offset));

private:
  int64_t magic_;
  int64_t version_;
  int64_t index_offset_;
};

// ====================================================================================
// Decoded output of one record. A record corresponds to one tablet within one
// (group, type) bucket file.
// ====================================================================================
struct ObSSMarkSweepDiagTabletMacros final
{
public:
  ObSSMarkSweepDiagTabletMacros();
  ~ObSSMarkSweepDiagTabletMacros() { reset(); }
  void reset() { tablet_.reset(); macro_ids_.reset(); }
  TO_STRING_KV(K_(tablet), "macro_cnt", macro_ids_.count());
public:
  ObSSGCTablet tablet_;
  common::ObArray<blocksstable::MacroBlockId> macro_ids_;
};

struct ObSSMarkSweepTabletMetaScns
{
public:
  ObSSMarkSweepTabletMetaScns();
  ~ObSSMarkSweepTabletMetaScns() { reset(); }
  void reset() { tablet_.reset(); meta_scns_.reset(); }
  TO_STRING_KV(K_(tablet), "scn_cnt", meta_scns_.count());
public:
  ObSSGCTablet tablet_;
  common::ObArray<share::SCN> meta_scns_;
};

struct ObSSLobMetaGCDiagRows final
{
public:
  ObSSLobMetaGCDiagRows();
  ~ObSSLobMetaGCDiagRows() { reset(); }
  void reset() { macro_ids_.reset(); }
  TO_STRING_KV("macro_cnt", macro_ids_.count());
public:
  common::ObArray<blocksstable::MacroBlockId> macro_ids_;
};

struct ObSSLobCtrlGCDiagRows final
{
public:
  ObSSLobCtrlGCDiagRows();
  ~ObSSLobCtrlGCDiagRows() { reset(); }
  void reset() { rows_.reset(); }
  TO_STRING_KV("row_cnt", rows_.count());
public:
  common::ObArray<ObSSOuttableLobCtrlCandidate> rows_;
};

// Raw Study watermarks from one compute node; not the GC-merged values.
struct ObSSMarkSweepDiagStudyNode final
{
  ObSSMarkSweepDiagStudyNode() : server_(), study_info_() {}
  void reset()
  {
    server_.reset();
    study_info_.min_dep_meta_scn_.reset();
    study_info_.min_major_snapshot_ = 0;
  }
  bool is_valid() const
  {
    return server_.is_valid() && study_info_.min_dep_meta_scn_.is_valid()
        && study_info_.min_major_snapshot_ >= 0;
  }
  TO_STRING_KV(K_(server), K_(study_info));

  common::ObAddr server_;
  ObSSGCTabletStudyInfo study_info_;
};

// One Study record covers a full tablet; the decoded result owns all node data.
struct ObSSMarkSweepDiagStudyResults final
{
  ObSSMarkSweepDiagStudyResults();
  ~ObSSMarkSweepDiagStudyResults() { reset(); }
  void reset() { tablet_.reset(); study_results_.reset(); }
  TO_STRING_KV(K_(tablet), "node_count", study_results_.count());

  ObSSGCTablet tablet_;
  common::ObArray<ObSSMarkSweepDiagStudyNode> study_results_;
};

// ====================================================================================
// ObSSMarkSweepDiagMgr — tenant/round-level diagnostic recorder.
//
// For wrongful-deletion troubleshooting it records, per (group, type), the macro block ids
// that mark-sweep GC actually saw and the del/kept meta_scns decision, into object storage.
// One whole mark-sweep round maintains a fixed number of hash buckets (object storage files)
// per (group, type); tablets are spread across buckets by ObSSGCTablet::hash() % bucket_cnt:
//   {root}/cluster_{id}/tenant_{id}/mark_sweep_gc_diag/round{id}/{del|kept}/{macros|tablet_meta_scns}/bkt{bucket}.T{obj_id}
//
// The manager is owned by ObSSMarkSweepCtx (one per round): init() once in PrepareTask, and
// complete()/abort() once in ObSSMarkSweepDag::report_result(). Multiple SortTasks write
// concurrently; each (group,type,bucket) stream has its own ObSpinLock.
//
// Writes happen at the END of process_one_tablet_ (after that tablet's DEL/KEPT groups
// succeed). Since execute_with_retry retries per tablet, a failed attempt records nothing,
// so each tablet appears exactly once -> no dedup needed.
//
// This is a best-effort diagnostic side-channel: callers must NOT propagate its failures
// into the GC main flow's ret.
// ====================================================================================
class ObSSMarkSweepDiagMgr final
{
public:
  static const int64_t GROUP_CNT = 2; // DEL / KEPT
  static const int64_t TYPE_CNT = static_cast<int64_t>(ObSSMarkSweepDiagFileType::MAX);
  static const int64_t DEFAULT_BUCKET_CNT = 100;

public:
  ObSSMarkSweepDiagMgr();
  ~ObSSMarkSweepDiagMgr();
  int init(const uint64_t tenant_id, const int64_t round_id, const int64_t bucket_cnt);
  // Register a round marker once before Study when the cluster supports it; all types share it.
  static int prepare_diag_task(const uint64_t tenant_id, const int64_t round_id);
  // Check the round marker before writing, except on legacy clusters; do not insert or rewrite it.
  int check_diag_task();
  void reset();
  bool is_inited() const { return is_inited_; }
  // The "diagnose enabled for this round" gate, owned by the manager (set from the tenant
  // config in PrepareTask). Combined with is_inited_ via is_ready_record().
  void set_diag_enabled(const bool enabled) { is_diag_enabled_ = enabled; }
  bool is_diag_enabled() const { return is_diag_enabled_; }
  // Self-documenting "ok to record diag info" predicate: config gate AND init success.
  bool is_ready_record() const { return is_diag_enabled_ && is_inited_; }
  // Sticky "any diag file failed during this round" flag and the first failure's ret code.
  // Set from multiple SortTask threads, read once in report_result()。
  bool has_diag_failed() const { return ATOMIC_LOAD(&has_diag_failed_); }
  int get_first_fail_ret() const { return ATOMIC_LOAD(&first_fail_ret_); }
  // Record a diag failure: set has_diag_failed_ and keep the first failure's ret code.
  void mark_diag_failed(const int ret_code);

  // Append one tablet's macro ids (in `group`) as one record to its hash bucket.
  int write_tablet_macros(const ObMarkSweepGroupType group,
                          const ObSSGCTablet &tablet,
                          const common::ObIArray<blocksstable::MacroBlockId> &macro_ids);
  // Append one tablet's meta_scns decision (in `group`) as one record to its hash bucket.
  int write_tablet_meta_scns(const ObMarkSweepGroupType group,
                             const ObSSGCTablet &tablet,
                             const common::ObIArray<share::SCN> &meta_scns);
  int write_lob_meta_kept(
      const common::ObIArray<blocksstable::MacroBlockId> &macro_ids);
  int write_lob_meta_kept_file(
      const ObSortedFileInfo &file,
      const int64_t file_buf_size);
  int write_lob_ctrl_rows(
      const ObSSMarkSweepDiagFileType type,
      const common::ObIArray<ObSSOuttableLobCtrlCandidate> &rows);
  int write_lob_ctrl_file(
      const ObSSMarkSweepDiagFileType type,
      const ObSSLobCtrlGCScanFile &file,
      const int64_t file_buf_size);
  int write_tablets_study_results(
      const int64_t bucket,
      const char *buf,
      const int64_t len,
      const common::ObIArray<int64_t> &record_lens);
  int complete_study_results(const int64_t bucket);
  int publish_study_diag(
      const uint64_t tenant_id,
      const int64_t round_id,
      const share::SCN &gc_meta_scn,
      const int input_ret,
      const ObSSGCStudyDiagInfo &diag_info);
  // Serialize and append Index + Tailer, then complete() every opened bucket file.
  int complete();
  // Abort every opened bucket file (used when the round fails / over-retries).
  void abort();

  // ===== record decode helpers (shared by readers / unit tests) =====
  // Decode one raw record payload into its tablet + items. The record buffer is the
  // serialized "tablet + cnt + items" produced by write_tablet_macros/meta_scns; the
  // decoded output owns its own item array memory, so `rec` may be freed afterwards.
  static int decode_macros_record(const common::ObString &rec,
                                  ObSSMarkSweepDiagTabletMacros &out);
  static int decode_meta_scns_record(const common::ObString &rec,
                                     ObSSMarkSweepTabletMetaScns &out);
  static int decode_lob_meta_kept_record(const common::ObString &rec,
                                         ObSSLobMetaGCDiagRows &out);
  static int decode_lob_ctrl_record(const common::ObString &rec,
                                    ObSSLobCtrlGCDiagRows &out);
  static bool is_lob_ctrl_type(const ObSSMarkSweepDiagFileType type);
  static int get_study_record_size(
      const ObSSGCTablet &tablet,
      const common::ObIArray<ObSSMarkSweepDiagStudyNode> &study_results,
      int64_t &size);
  static int encode_study_record(
      const ObSSGCTablet &tablet,
      const common::ObIArray<ObSSMarkSweepDiagStudyNode> &study_results,
      char *buf,
      const int64_t buf_len,
      int64_t &pos);
  static int decode_study_record(
      const common::ObString &rec,
      ObSSMarkSweepDiagStudyResults &out);

  // ===== path / macro-id helpers =====
  // Build a MacroBlockId encoding one (round, group, type, bucket) diag file.
  static int get_diag_macro_id(const int64_t round_id,
                               const ObMarkSweepGroupType group,
                               const ObSSMarkSweepDiagFileType type,
                               const int64_t bucket,
                               blocksstable::MacroBlockId &macro_id);
  // {root}/cluster_{id}/tenant_{id}/mark_sweep_gc_diag — used by cleanup task for directory listing
  static int build_diag_root_dir(const uint64_t tenant_id, char *buf, const int64_t buf_len);

  // storage dest/id for the data device (object storage), reused by reader & cleanup.
  static int get_storage_dest_and_id(share::ObBackupDest &storage_dest, uint64_t &storage_id);

private:
  // Align with the current ObStorageBufferedMultiPartWriter::PART_SIZE_THRESHOLD.
  // Batching target only; a full record may slightly exceed it. Re-check when the underlying threshold changes.
  static const int64_t STUDY_BATCH_BYTES = 6L * 1024L * 1024L;
  struct StudyDiagBuffer;
  struct StudyDiagTabletIndex
  {
    StudyDiagTabletIndex() : server_idx_(0), tablet_idx_(0) {}
    StudyDiagTabletIndex(
        const int64_t server_idx,
        const int64_t tablet_idx)
      : server_idx_(server_idx), tablet_idx_(tablet_idx) {}
    TO_STRING_KV(K_(server_idx), K_(tablet_idx));
    int64_t server_idx_;
    int64_t tablet_idx_;
  };
  struct StudyDiagStat
  {
    StudyDiagStat();
    void record_failure(
        const int ret_code,
        const char *stage);
    int first_fail_ret_;
    const char *first_fail_stage_;
    int64_t input_result_count_;
    int64_t input_tablet_count_;
    int64_t append_record_count_;
    int64_t publish_cost_us_;
  };
  // One open multipart-upload file for a (group, type, bucket) diag file, guarded by its lock.
  struct ObSSMarkSweepDiagFile final
  {
  public:
    ObSSMarkSweepDiagFile();
    ~ObSSMarkSweepDiagFile() { reset(); }
    void reset();
    TO_STRING_KV(KP_(device_handle), K_(fd), K_(offset), K_(is_opened), K_(is_finished),
        "rec_cnt", index_.count());
  public:
    common::ObIODevice *device_handle_;
    common::ObIOFd fd_;
    int64_t offset_;             // total bytes appended (= next part offset)
    bool is_opened_;
    bool is_finished_;          // After a Study bucket succeeds or fails, further writes are rejected.
    ObSSMarkSweepDiagIndex index_; // per-record byte lengths, written out at complete()
    common::ObSpinLock lock_;
  };

  int validate_study_diag_(
      const ObSSGCStudyDiagInfo &diag_info,
      StudyDiagStat &stat);
  int serialize_study_diag_record_(
      const ObSSGCStudyDiagInfo &diag_info,
      const StudyDiagTabletIndex &index,
      StudyDiagBuffer &buffer,
      StudyDiagStat &stat);
  int serialize_study_diag_batch_(
      const ObSSGCStudyDiagInfo &diag_info,
      StudyDiagTabletIndex &next_tablet,
      StudyDiagBuffer &buffer,
      StudyDiagStat &stat);
  int write_study_diag_(
      const uint64_t tenant_id,
      const int64_t round_id,
      const ObSSGCStudyDiagInfo &diag_info,
      StudyDiagStat &stat);
  static void report_study_diag_(
      const uint64_t tenant_id,
      const int64_t round_id,
      const share::SCN &gc_meta_scn,
      const int input_ret,
      const bool can_publish,
      const StudyDiagStat &stat);

  // Identify a Study file slot; bucket_cnt is passed explicitly for init before members are assigned.
  static bool is_study_results_file_(
      const int64_t file_idx,
      const int64_t bucket_cnt);
  int64_t diag_file_idx_(const ObMarkSweepGroupType group,
                         const ObSSMarkSweepDiagFileType type,
                         const int64_t bucket) const;
  int64_t calc_bucket_(const ObSSGCTablet &tablet) const;
  int64_t calc_lob_bucket_(const blocksstable::MacroBlockId &macro_id) const;
  // serialize one record (tablet + items) and append it to its bucket diag_file under the lock.
  int write_record_(const ObMarkSweepGroupType group,
                    const ObSSMarkSweepDiagFileType type,
                    const ObSSGCTablet &tablet,
                    const char *buf,
                    const int64_t len);
  int write_record_(const ObSSMarkSweepDiagFileType type,
                    const int64_t bucket,
                    const char *buf,
                    const int64_t len);
  int write_lob_meta_kept_record_(
      const int64_t bucket,
      const common::ObIArray<blocksstable::MacroBlockId> &macro_ids);
  int write_lob_ctrl_record_(
      const ObSSMarkSweepDiagFileType type,
      const int64_t bucket,
      const common::ObIArray<ObSSOuttableLobCtrlCandidate> &rows);
  int ensure_open_(const ObMarkSweepGroupType group,
                   const ObSSMarkSweepDiagFileType type,
                   const int64_t bucket,
                   ObSSMarkSweepDiagFile &diag_file);
  int append_record_(ObSSMarkSweepDiagFile &diag_file, const char *buf, const int64_t len);
  int append_records_(
      ObSSMarkSweepDiagFile &diag_file,
      const char *buf,
      const int64_t len,
      const common::ObIArray<int64_t> &record_lens);
  int complete_(ObSSMarkSweepDiagFile &diag_file);
  void abort_(ObSSMarkSweepDiagFile &diag_file);

public:
  // Read a diag file's Tailer + Index, returning each record's byte length and the
  // total length of the data region. Does NOT read the data region itself, so the
  // caller can pread record payloads in bounded batches. `record_lens` memory comes
  // from `allocator`.
  static int parse_tailer_and_index(const common::ObString &uri,
                                    const share::ObBackupDest &storage_dest,
                                    const uint64_t storage_id,
                                    common::ObIAllocator &allocator,
                                    common::ObIArray<int64_t> &record_lens,
                                    int64_t &data_len);

private:
  bool is_inited_;
  bool is_diag_enabled_;  // config gate for this round; see is_ready_record()
  bool is_diag_task_prepared_;
  bool has_diag_failed_;  // sticky: any diag file failed this round
  int first_fail_ret_;    // first diag failure's ret code
  uint64_t tenant_id_;
  int64_t cluster_id_;
  int64_t round_id_;
  int64_t bucket_cnt_;
  int64_t diag_file_cnt_; // GROUP_CNT * TYPE_CNT * bucket_cnt_
  ObSSMarkSweepDiagFile *diag_file_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepDiagMgr);
};

// ====================================================================================
// ObSSGCStudyDiagGuard — sole Study-diagnose entry on the GC main path.
//
// Ctor evaluates the Study gate (tenant config + cluster version) and, when enabled, binds the
// collector to the current thread. The study path uses ObSSGCStudyDiagInfo static wrappers, so
// diag_info need not be threaded through call signatures. prepare() registers the round marker
// before Study; finish() publishes. Callers must isolate diagnose errors. Construct on the
// thread that actually runs Study.
// ====================================================================================
class ObSSGCStudyDiagGuard final
{
public:
  ObSSGCStudyDiagGuard(const uint64_t tenant_id, const int64_t round_id);
  ~ObSSGCStudyDiagGuard() { diag_info_.reset(); }
  // Call once before Study when diagnose is enabled (same gate as collection).
  // Registration failure disables diagnose collection, not GC.
  int prepare();
  // Call before creating the DAG to publish and free raw results. round_ret is the study + checkpoint merge result.
  // Publish only when round_ret succeeds; the caller re-checks executor/round after finish before creating the DAG.
  // Disabled or repeated calls return success. Returns the first diagnose error; the caller must not abort GC.
  int finish(const share::SCN &gc_meta_scn, const int round_ret);

private:
  static bool check_diag_enabled_(const uint64_t tenant_id);

  bool is_enabled_;
  bool is_finished_;
  uint64_t tenant_id_;
  int64_t round_id_;
  ObSSGCStudyDiagInfo diag_info_;
  ObSSGCStudyDiagInfo::Scope scope_; // Declared after diag_info_ so the object exists before binding
  DISALLOW_COPY_AND_ASSIGN(ObSSGCStudyDiagGuard);
};

// ====================================================================================
// ObSSMarkSweepDiagFileReader — streaming reader of one (tenant, round, group, type,
// bucket) diag file. The URI is built from an explicit tenant_id (NOT MTL_ID()), so a
// reader running in any tenant context (e.g. sys) can read another tenant's diag files.
//
// To bound memory on large files, records are returned in batches: next_record_batch() preads
// a contiguous run of whole records whose total size is <= MAX_BATCH_BYTES (a single
// oversized record forms its own batch). Use it as:
//   ObSSMarkSweepDiagFileReader reader;
//   reader.init(tenant_id, round_id, group, type, bucket);
//   while (OB_SUCC(reader.next_record_batch(allocator, records))) { ... }  // OB_ITER_END ends
// ====================================================================================
class ObSSMarkSweepDiagFileReader final
{
public:
  static const int64_t MAX_BATCH_BYTES = 2LL << 20; // 2MB

  ObSSMarkSweepDiagFileReader();
  ~ObSSMarkSweepDiagFileReader() { reset(); }
  void reset();
  // Build the URI from explicit tenant_id, read Tailer + Index. Returns the per-record
  // layout but reads no data. The parsed record-length array is owned internally.
  int init(const uint64_t tenant_id,
           const int64_t round_id,
           const ObMarkSweepGroupType group,
           const ObSSMarkSweepDiagFileType type,
           const int64_t bucket);
  bool is_inited() const { return is_inited_; }
  // pread the next batch of whole records (<= MAX_BATCH_BYTES) and split into per-record
  // payloads; `records` and their backing buffer come from `allocator`. Returns
  // OB_ITER_END when all records are consumed.
  int next_record_batch(common::ObIAllocator &allocator, common::ObIArray<common::ObString> &records);

private:
  bool is_inited_;
  share::ObBackupDest storage_dest_;
  uint64_t storage_id_;
  char uri_[common::MAX_PATH_SIZE];
  common::ObArenaAllocator index_allocator_; // owns record_lens_
  common::ObArray<int64_t> record_lens_;     // per-record byte lengths (from the file Index)
  int64_t data_len_;       // total bytes of the data region
  int64_t next_record_idx_;   // next record index to emit
  int64_t next_offset_;    // byte offset in the data region of next_record_idx_
  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepDiagFileReader);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_MARK_SWEEP_DIAG_MGR_H_

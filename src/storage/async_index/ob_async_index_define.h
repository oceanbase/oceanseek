/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_DEFINE_H_
#define OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_DEFINE_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_ls_id.h"
#include "rootserver/ddl_task/ob_ddl_task.h"  // rootserver::ObDDLSliceInfo
#include "storage/tablet/ob_async_index_refresh_info.h"  // ObTabletAsyncIndexRefreshInfo

namespace oceanbase
{
namespace storage
{

class ObLSAsyncIndexScheduler;

// 异步索引逻辑版本的初始值。
static const int64_t ASYNC_INDEX_INITIAL_VERSION = 0;

// IncMajor 允许的最小 logical base，当前为 0，方案落地后调整为 1。
static const int64_t ASYNC_INDEX_MIN_INC_MAJOR_BASE_VERSION =
    ASYNC_INDEX_INITIAL_VERSION;

inline bool is_valid_async_index_version_range(
    const bool is_inc_major_sstable,
    const int64_t base_version,
    const int64_t snapshot_version)
{
  return is_inc_major_sstable
      ? (base_version >= ASYNC_INDEX_MIN_INC_MAJOR_BASE_VERSION
          && snapshot_version > base_version)
      : (base_version >= ASYNC_INDEX_INITIAL_VERSION
          && snapshot_version >= ASYNC_INDEX_INITIAL_VERSION);
}

// The type of async index refresh trigger.
enum ObAsyncRefreshType
{
  ASYNC_REFRESH_TYPE_PERIOD = 0,       // periodic incremental refresh
  ASYNC_REFRESH_TYPE_CREATE_INDEX = 1, // triggered by create index (initial build)
  ASYNC_REFRESH_TYPE_MANUAL = 2, // triggered by direct refresh
  ASYNC_REFRESH_TYPE_OFFLINE_DDL = 3, // offline DDL pre-swap refresh on hidden table
  ASYNC_REFRESH_TYPE_MAX
};

inline bool is_valid_async_refresh_type(const ObAsyncRefreshType type)
{
  return type >= ASYNC_REFRESH_TYPE_PERIOD && type < ASYNC_REFRESH_TYPE_MAX;
}

inline const char *async_refresh_type_to_str(const ObAsyncRefreshType type)
{
  const char *str = "UNKNOWN";
  switch (type) {
    case ASYNC_REFRESH_TYPE_PERIOD: str = "PERIOD"; break;
    case ASYNC_REFRESH_TYPE_CREATE_INDEX: str = "CREATE_INDEX"; break;
    case ASYNC_REFRESH_TYPE_MANUAL: str = "MANUAL"; break;
    case ASYNC_REFRESH_TYPE_OFFLINE_DDL: str = "OFFLINE_DDL"; break;
    default: break;
  }
  return str;
}

// ======================== SCN <-> snapshot conversion ========================
// The scheduler reasons in share::SCN; the per-tablet Async Refresh Info MDS
// (ObTabletAsyncIndexRefreshInfo) stores plain int64_t snapshot versions. These
// two helpers are the single canonical bridge between the two views.
//
// Convention:
//   - an invalid SCN or SCN::min_scn() maps to ASYNC_INDEX_INITIAL_VERSION
//     ("initial logical version"); append_refresh_snapshot / get_next_refresh_snapshot
//     both treat a non-positive snapshot as "absent".
//   - SCN::max_scn() maps to INT64_MAX and round-trips back to max.
//   - any other SCN maps losslessly via get_val_for_tx() / convert_for_tx().
inline int64_t async_index_scn_to_snapshot(const share::SCN &scn)
{
  int64_t snapshot = ASYNC_INDEX_INITIAL_VERSION;
  if (!scn.is_valid() || scn.is_min()) {
    snapshot = ASYNC_INDEX_INITIAL_VERSION;
  } else if (scn.is_max()) {
    snapshot = INT64_MAX;
  } else {
    snapshot = scn.get_val_for_tx();
  }
  return snapshot;
}

inline share::SCN async_index_snapshot_to_scn(const int64_t snapshot)
{
  share::SCN scn;
  if (ASYNC_INDEX_INITIAL_VERSION == snapshot) {
    // The initial logical version has no incremental start point, so it maps
    // to min_scn and takes the COMPLETE refresh path.
    scn = share::SCN::min_scn();
  } else if (snapshot < 0) {
    // Keep corrupted or otherwise invalid persisted snapshots invalid.
  } else if (INT64_MAX == snapshot) {
    scn = share::SCN::max_scn();
  } else {
    scn.convert_for_tx(snapshot);
  }
  return scn;
}

// ======================== Constants ========================
static const int64_t OB_ASYNC_INDEX_TASK_QUEUE_SIZE = 1024;
static const int64_t OB_ASYNC_INDEX_DEFAULT_REFRESH_INTERVAL_US = 5L * 1000L * 1000L; // 5s
static const int64_t OB_ASYNC_INDEX_TIMER_INTERVAL_US = 1L * 1000L * 1000L;           // 1s check cycle, to adapt the minimum of ASYNC_REFRESH_INTERVAL
static const int64_t OB_ASYNC_INDEX_MAX_RETRY_INTERVAL_US = 5L * 60L * 1000L * 1000L; // 5 min
static const int64_t OB_ASYNC_INDEX_BASE_RETRY_INTERVAL_US = 5L * 1000L * 1000L;      // 5s base
static const int64_t OB_ASYNC_INDEX_IMMEDIATE_RETRY_COUNT = 3;  // first 3 retries are immediate
static const int64_t OB_ASYNC_INDEX_MAX_RETRY_CNT = 16;  // give up after this many retries (task falls into observable terminal state)
static const int64_t OB_ASYNC_INDEX_MAX_RETRY_LIST_LEN = 1024;  // per-LS retry list cap; overflow rejects new nodes (regenerated next round)
static const int64_t OB_ASYNC_INDEX_SSTABLE_COUNT_THRESHOLD = 48;
// 手动刷新读取到的 checkpoint 数量超过该阈值时，拒绝本次请求。
static const int64_t OB_ASYNC_INDEX_MANUAL_REFRESH_CHECKPOINT_THRESHOLD = 10;
static const int64_t OB_ASYNC_INDEX_MIN_PARALLELISM = 1;
static const int64_t OB_ASYNC_INDEX_FINISHED_RING_SIZE = 256;  // recent finished task stats kept for observability
static const int64_t OB_ASYNC_INDEX_REFRESH_LOCK_TIMEOUT_US = 300L * 1000L;        // 100ms for incremental refresh
static const int64_t OB_ASYNC_INDEX_DDL_LOCK_TIMEOUT_US = 1L * 1000L * 1000L;      // 1s for DDL post-build
static const int64_t DEFAULT_ASYNC_INDEX_REFRESH_DOP_PERCENT = 25;
static const int64_t OB_ASYNC_INDEX_SLOW_THRESHOLD_PERCENT = 80;
static const int64_t OB_ASYNC_INDEX_FAST_THRESHOLD_PERCENT = 60;
static const int64_t OB_ASYNC_INDEX_DOP_STATE_TTL_US = 10L * 60L * 1000L * 1000L;

// Only failures recoverable by a new transaction may reuse the original SQL.
// All other failures release the batch for the next periodic schema/SCN check.
inline bool is_async_index_retryable_error(const int err)
{
  return OB_TIMEOUT == err
      || OB_TRANS_TIMEOUT == err
      || OB_TRANS_RPC_TIMEOUT == err
      || OB_RPC_CONNECT_ERROR == err
      || OB_RPC_SEND_ERROR == err
      || OB_RPC_POST_ERROR == err
      || OB_TRANS_ROLLBACKED == err
      || OB_TRANS_NEED_ROLLBACK == err
      || OB_TRANS_STMT_NEED_RETRY == err;
}

// Reserve half the free slots for normal scheduling. Alternate the odd slot
// so neither class starves when only one worker slot is freed per timer tick.
inline int64_t calc_async_index_retry_admission_budget(const int64_t free_slots, const int64_t round)
{
  return free_slots <= 0 ? 0 : free_slots / 2 + (free_slots % 2) * (round % 2);
}

// Calculate retry delay in microseconds based on retry count.
// First OB_ASYNC_INDEX_IMMEDIATE_RETRY_COUNT retries: 0 (immediate).
// After that: 5s, 10s, 20s, 40s, ..., capped at 5 min.
inline int64_t calc_async_index_retry_delay_us(const int64_t retry_cnt)
{
  if (retry_cnt <= OB_ASYNC_INDEX_IMMEDIATE_RETRY_COUNT) {
    return 0;
  }
  const int64_t exp = retry_cnt - OB_ASYNC_INDEX_IMMEDIATE_RETRY_COUNT - 1;
  int64_t delay = OB_ASYNC_INDEX_BASE_RETRY_INTERVAL_US;
  for (int64_t i = 0; i < exp && delay < OB_ASYNC_INDEX_MAX_RETRY_INTERVAL_US; ++i) {
    delay = delay * 2;
  }
  if (delay > OB_ASYNC_INDEX_MAX_RETRY_INTERVAL_US) {
    delay = OB_ASYNC_INDEX_MAX_RETRY_INTERVAL_US;
  }
  return delay;
}

// Per-(refresh_task_id) execution context, registered before the inner
// refresh SQL runs and read back by ObPxSubCoord::start_ddl() during execution.
// The synthetic refresh_task_id is NOT a real __all_ddl_task_status record, so
// start_ddl cannot use ObDDLUtil::get_data_information; it reads this context
// from ObAsyncIndexService's node-local map instead (mirrors the vector index
// is_vec_tablet_rebuild path / ObVectorIndexTmpInfo).
struct ObAsyncIndexRefreshTmpInfo final
{
  ObAsyncIndexRefreshTmpInfo()
    : start_refresh_scn_(), snapshot_version_(0), schema_version_(0), ddl_build_id_(0), ddl_slice_info_(),
      allocator_(lib::ObMemAttr(common::OB_SERVER_TENANT_ID, "AsyncIdxSlice")),
      ref_cnt_(0), skipped_rows_(0) {}
  ~ObAsyncIndexRefreshTmpInfo() {}
  void init(const uint64_t tenant_id) {
    allocator_.set_attr(lib::ObMemAttr(tenant_id, "AsyncIdxSlice"));
  }
  void reset() { start_refresh_scn_.reset(); snapshot_version_ = 0; schema_version_ = 0; ddl_build_id_ = 0; ddl_slice_info_.reset(); ATOMIC_STORE(&skipped_rows_, 0LL); }
  void inc_ref() { ATOMIC_INC(&ref_cnt_); }
  int64_t dec_ref() { return ATOMIC_AAF(&ref_cnt_, -1); }
  int64_t get_ref_cnt() const { return ATOMIC_LOAD(&ref_cnt_); }
  void add_skipped_rows(const int64_t cnt) { ATOMIC_AAF(&skipped_rows_, cnt); }
  int64_t get_skipped_rows() const { return ATOMIC_LOAD(&skipped_rows_); }
  TO_STRING_KV(K_(start_refresh_scn), K_(snapshot_version), K_(schema_version), K_(ddl_build_id), K_(ddl_slice_info), K_(ref_cnt), K_(skipped_rows));

  share::SCN start_refresh_scn_; // refresh window lower bound, i.e. previous published SCN
  int64_t snapshot_version_;  // == refresh SQL's AS OF SNAPSHOT (target_refresh_scn get_val_for_sql)
  int64_t schema_version_;    // data table schema version captured at SQL generation time
  int64_t ddl_build_id_;      // physical execution ID used by all slices of this refresh
  // Sample range cuts produced by ObDynamicSamplePieceMsgCtx::build_whole_msg during
  // the refresh SQL's PX sample scan, stashed here (deep-copied into allocator_) the
  // same way the vector path stashes them in ObVectorIndexTmpInfo.
  // ObColumnClusteredDag::update_tablet_range_count reads them back via
  // ObAsyncIndexService::get_inner_sql_slice_info during PX execution.
  rootserver::ObDDLSliceInfo ddl_slice_info_;
  common::ObArenaAllocator allocator_;  // owns deep-copied ddl_slice_info_ contents; init() sets tenant_id
  volatile int64_t ref_cnt_;  // reference count: protects against UAF between get/release
  volatile int64_t skipped_rows_;  // 增量刷新跳过行计数（PX worker 原子累加）
};

// ======================== Async Index Refresh Dependency Structures ========================
//
// These structures carry the schema and tablet mapping information needed to
// construct incremental refresh SQL. They are built ONCE per timer round by
// get_async_index_refresh_deps() and consumed by the refresh scheduling loop.
//
// String lifetime: all ObString members are deep-copied into the arena
// allocator owned by ObAsyncIndexRefreshDeps, so they remain valid as long as
// the ObAsyncIndexRefreshDeps object is alive (typically one timer round).

// Data tablet information for one partition of a data table.
// For non-partitioned tables there is exactly one entry;
// for partitioned tables there is one entry per partition.
struct ObAsyncIndexDataTabletInfo
{
  ObAsyncIndexDataTabletInfo() { reset(); }
  void reset()
  {
    data_tablet_id_.reset();
    partition_name_.reset();
    data_ls_id_.reset();
  }
  bool is_valid() const { return data_tablet_id_.is_valid(); }
  TO_STRING_KV(K_(data_tablet_id), K_(partition_name), K_(data_ls_id));

  common::ObTabletID data_tablet_id_;
  common::ObString partition_name_;  // partition name (empty if non-partitioned)
  // LS that owns this data tablet, filled by the dep provider via the
  // tablet->LS mapping when building deps. When valid, the scheduler routes the
  // tablet to its owning leader LS in O(1); when invalid (e.g. provider not yet
  // populating it, or unit tests), the scheduler falls back to probing each
  // leader LS via ObLS::get_tablet().
  share::ObLSID data_ls_id_;
};

// Complete information for a single async index on a data table, including
// both schema metadata and per-partition tablet mappings.
// Physical async refresh targets are represented as independent ObAsyncIndexInfo
// entries. The shared delete index is identified by is_delete_index_ == true.
// A data table has at most one delete index.
//
// tablet_ids_[t] is the index tablet corresponding to
// ObAsyncIndexDataTableInfo::data_tablet_infos_[t] (same partition).
struct ObAsyncIndexInfo
{
  ObAsyncIndexInfo() { reset(); }
  void reset()
  {
    index_table_id_ = OB_INVALID_ID;
    def_index_table_id_ = OB_INVALID_ID;
    index_type_ = share::schema::INDEX_TYPE_IS_NOT;
    index_table_name_.reset();
    select_columns_.reset();
    source_columns_.reset();
    partition_key_columns_.reset();
    rowkey_columns_.reset();
    order_columns_.reset();
    enable_newsort_ = true;
    is_delete_index_ = false;
    is_refresh_on_error_skip_ = false;
    tablet_ids_.reset();
    partition_names_.reset();
  }
  bool is_valid() const
  {
    return OB_INVALID_ID != index_table_id_
        && !index_table_name_.empty();
  }
  TO_STRING_KV(K_(index_table_id), K_(index_table_name),
               K_(def_index_table_id), K_(index_type),
               K_(enable_newsort),
               K_(is_delete_index),
               "select_col_cnt", select_columns_.count(),
               "source_col_cnt", source_columns_.count(),
               "rowkey_col_cnt", rowkey_columns_.count(),
               "order_col_cnt", order_columns_.count(),
               "tablet_cnt", tablet_ids_.count());

  // --- schema metadata ---
  uint64_t index_table_id_;
  uint64_t def_index_table_id_;                            // async Search definition table, invalid for FTS/delete
  share::schema::ObIndexType index_type_;                  // physical target type
  common::ObString index_table_name_;                      // async index table name
  common::ObSEArray<common::ObString, 8> select_columns_;  // columns to SELECT from data table into index
  common::ObSEArray<common::ObString, 8> source_columns_;  // async Search input columns from the definition table
  common::ObSEArray<common::ObString, 4> partition_key_columns_;
  // 物理索引 rowkey。Delete Index 固定为 [__pk_increment]，同时作为 delete SQL 的 docid。
  common::ObSEArray<common::ObString, 4> rowkey_columns_;
  // 刷新 SQL 的排序列；NESTED Async Search 在四列物理 rowkey 后追加 scalar nested key。
  common::ObSEArray<common::ObString, 5> order_columns_;
  bool enable_newsort_;                                   // true when all ORDER BY columns support encoded sorting
  bool is_delete_index_;                                   // true if this is the delete buffer index itself
  bool is_refresh_on_error_skip_;                          // REFRESH_ON_ERROR=SKIP

  // --- per-partition tablet mapping ---
  // tablet_ids_[t] corresponds to ObAsyncIndexDataTableInfo::data_tablet_infos_[t]
  common::ObSEArray<common::ObTabletID, 16> tablet_ids_;
  // partition_names_[t] is the index partition name for tablet_ids_[t]
  common::ObSEArray<common::ObString, 16> partition_names_;
};

// Complete refresh dependency information for one data table.
struct ObAsyncIndexDataTableInfo
{
  ObAsyncIndexDataTableInfo() { reset(); }
  void reset()
  {
    data_table_id_ = OB_INVALID_ID;
    database_name_.reset();
    data_table_name_.reset();
    schema_version_ = 0;
    refresh_interval_us_ = OB_ASYNC_INDEX_DEFAULT_REFRESH_INTERVAL_US;
    data_tablet_infos_.reset();
    index_infos_.reset();
  }
  bool is_valid() const
  {
    return OB_INVALID_ID != data_table_id_
        && !database_name_.empty()
        && !data_table_name_.empty()
        && !data_tablet_infos_.empty()
        && !index_infos_.empty();
  }
  TO_STRING_KV(K_(data_table_id), K_(database_name), K_(data_table_name),
               K_(schema_version), K_(refresh_interval_us),
               "data_tablet_cnt", data_tablet_infos_.count(),
               "index_cnt", index_infos_.count());

  uint64_t data_table_id_;
  common::ObString database_name_;
  common::ObString data_table_name_;
  int64_t schema_version_;
  // Configured interval used as the adaptive-DOP latency target.
  int64_t refresh_interval_us_;
  // Per-partition data tablet info, ordered by partition position.
  common::ObSEArray<ObAsyncIndexDataTabletInfo, 16> data_tablet_infos_;
  // Per-index info (schema + tablet mapping).
  // index_infos_[i].tablet_ids_[t] is the index tablet for data_tablet_infos_[t].
  common::ObSEArray<ObAsyncIndexInfo, 4> index_infos_;
};

// Lightweight scheduling metadata collected before the expensive schema deep
// copy. refresh_interval_us_ is expressed in microseconds; -1 disables timer
// refresh.
struct ObAsyncTableRefreshInterval
{
  ObAsyncTableRefreshInterval()
    : data_table_id_(OB_INVALID_ID),
      refresh_interval_us_(0)
  {}
  ObAsyncTableRefreshInterval(const uint64_t data_table_id, const int64_t refresh_interval_us)
    : data_table_id_(data_table_id),
      refresh_interval_us_(refresh_interval_us)
  {}
  bool is_valid() const
  {
    return OB_INVALID_ID != data_table_id_
        && (-1 == refresh_interval_us_
            || (refresh_interval_us_ >= 1L * 1000L * 1000L
                && refresh_interval_us_ <= 60L * 1000L * 1000L));
  }
  TO_STRING_KV(K_(data_table_id), K_(refresh_interval_us));

  uint64_t data_table_id_;
  int64_t refresh_interval_us_;
};

// Tenant-level collection of all async index refresh dependencies.
// Built once per timer round and consumed by the scheduling loop.
struct ObAsyncIndexRefreshDeps
{
  ObAsyncIndexRefreshDeps()
    : allocator_("AsyncIdxDeps"),
      data_table_infos_()
  {}
  void reset()
  {
    data_table_infos_.reset();
    allocator_.reset();
  }
  bool empty() const { return data_table_infos_.empty(); }
  TO_STRING_KV("data_table_cnt", data_table_infos_.count());

  // Arena allocator for deep-copying strings from schema guard.
  common::ObArenaAllocator allocator_;
  common::ObSEArray<ObAsyncIndexDataTableInfo, 8> data_table_infos_;
};

// ======================== ObRefreshTaskStat ========================
// Statistics and context for a single async index refresh task.
struct ObRefreshTaskStat
{
  ObRefreshTaskStat() { reset(); }
  ~ObRefreshTaskStat() {}

  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    data_table_id_ = OB_INVALID_ID;
    ls_id_.reset();
    data_tablet_id_.reset();
    tablet_id_.reset();
    data_transfer_seq_ = -1;
    index_transfer_seq_ = -1;
    refresh_id_ = 0;
    refresh_task_id_ = 0;
    retry_cnt_ = 0;
    refresh_type_ = ASYNC_REFRESH_TYPE_PERIOD;
    queue_time_ = 0;
    start_time_ = 0;
    end_time_ = 0;
    affected_rows_ = 0;
    start_refresh_scn_.reset();
    target_refresh_scn_.reset();
    result_ = OB_SUCCESS;
    refresh_parallelism_ = OB_ASYNC_INDEX_MIN_PARALLELISM;
    schema_version_ = 0;
    leader_epoch_ = 0;
    is_partition_local_ = false;
    is_delete_index_ = false;
    is_complete_refresh_ = false;
    is_refresh_on_error_skip_ = false;
    skipped_rows_ = 0;
    trace_id_buf_[0] = '\0';
    trace_id_len_ = 0;
    is_search_index_ = false;
  }

  bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_
        && OB_INVALID_ID != data_table_id_
        && ls_id_.is_valid()
        && data_tablet_id_.is_valid()
        && tablet_id_.is_valid()
        && target_refresh_scn_.is_valid()
        && is_valid_async_refresh_type(refresh_type_);
  }

  TO_STRING_KV(K_(tenant_id),
               K_(data_table_id),
               K_(ls_id),
               K_(data_tablet_id),
               K_(tablet_id),
               K_(data_transfer_seq),
               K_(index_transfer_seq),
               K_(refresh_id),
               K_(refresh_task_id),
               K_(retry_cnt),
               "refresh_type", async_refresh_type_to_str(refresh_type_),
               K_(queue_time),
               K_(start_time),
               K_(end_time),
               K_(affected_rows),
               K_(start_refresh_scn),
               K_(target_refresh_scn),
               K_(result),
               K_(refresh_parallelism),
               K_(schema_version),
               K_(leader_epoch),
               K_(is_partition_local),
               K_(is_delete_index),
               K_(is_complete_refresh),
               K_(is_search_index));

  uint64_t tenant_id_;
  uint64_t data_table_id_;
  share::ObLSID ls_id_;                   // LS that owns this tablet
  common::ObTabletID data_tablet_id_;  // data table tablet id
  common::ObTabletID tablet_id_;       // index table tablet id
  int64_t data_transfer_seq_;          // generation observed during scheduling
  int64_t index_transfer_seq_;
  int64_t refresh_id_;                 // monotonically increasing refresh batch id
  int64_t refresh_task_id_;            // synthetic unique id baked into the SQL's opt_param('ddl_task_id');
                                       // used as the key to register/lookup snapshot & schema version
  int64_t retry_cnt_;
  ObAsyncRefreshType refresh_type_;
  int64_t queue_time_;                 // timestamp when task entered the queue
  int64_t start_time_;                 // timestamp when task execution started
  int64_t end_time_;                   // timestamp when task execution finished
  int64_t affected_rows_;              // actual rows written to index tablet
  share::SCN start_refresh_scn_;       // begin SCN for this refresh
  share::SCN target_refresh_scn_;      // target SCN for this refresh
  int result_;                         // execution result code
  int64_t refresh_parallelism_;        // requested DOP passed to parallel(N)
  int64_t schema_version_;             // data table schema version captured at SQL generation time;
                                       // re-validated before handle() executes to detect DDL drift
  int64_t leader_epoch_;               // LS-level leader epoch stamped when the task was enqueued;
                                       // re-validated before handle() executes so residual tasks
                                       // from a previous leader term on this LS are dropped
  bool is_partition_local_;            // true when the refresh target uses partition-local DDL
  bool is_delete_index_;               // true when the refresh target is the shared delete index
  bool is_complete_refresh_;           // true => COMPLETE (full snapshot) refresh; executor must
                                       // run it as a plain (non-mview) snapshot scan
  bool is_refresh_on_error_skip_;      // REFRESH_ON_ERROR=SKIP
  int64_t skipped_rows_;               // 本轮刷新被跳过的行数
  char trace_id_buf_[common::OB_MAX_TRACE_ID_BUFFER_SIZE];  // 刷新 SQL 的 trace_id
  int32_t trace_id_len_;
  bool is_search_index_;               // 当前刷新目标是否为 Search Data
};

// ======================== 行级数据错误白名单 ========================
// 仅因行数据内容导致的永久性错误允许在 REFRESH_ON_ERROR=SKIP 模式下跳过。
// 暂态错误（超时、主副本变更等）不在此列，必须向上传播以触发重试。
OB_INLINE bool is_async_index_row_level_data_error(const int err)
{
  return OB_INVALID_ARGUMENT == err            // 文本编码损坏、charset tolower 失败、LOB locator 损坏
      || OB_NOT_SUPPORTED == err               // 文档超长（>4GB）、不支持的 charset/LOB/嵌套数组
      || OB_UNEXPECT_INTERNAL_ERROR == err     // FTS parser 无法解析的字节序列（IK char_len==0）
      || OB_ERR_INCORRECT_STRING_VALUE == err  // charset_convert/wc_mb 编码失败（非法字节序列）
      || OB_SIZE_OVERFLOW == err               // JSON 嵌套过深、编码缓冲区不足
      || OB_ERR_INVALID_JSON_TEXT == err       // Search 索引：JSON binary 损坏/不可解析
      || OB_ERR_JSON_OUT_OF_DEPTH == err       // Search 索引：JSON 嵌套层数超限
      || OB_ERR_TOO_LONG_KEY_LENGTH == err;    // Search 索引：JSON path 编码超长（>2KB）
}

// ======================== ObAsyncIndexRefreshTaskParam ========================
// Bundles everything needed to schedule a single async index incremental
// refresh task. Passed by const-ref down the scheduling chain
// (ObTenantAsyncIndexRefresher -> ObAsyncIndexRefreshWorker -> ObAsyncIndexRefreshTask)
// so callers no longer hand-thread a long parameter list.
//
// sql_ and ls_scheduler_ are referenced, not owned: they must outlive the
// scheduling call. sql_ is the pre-generated refresh SQL (deep-copied into the
// task by set_task); ls_scheduler_ owns the LS-level leader epoch and retry
// list (may be null in unit tests).
struct ObAsyncIndexRefreshTaskParam
{
  ObAsyncIndexRefreshTaskParam()
    : tenant_id_(OB_INVALID_TENANT_ID),
      data_table_id_(OB_INVALID_ID),
      ls_id_(),
      data_tablet_id_(),
      index_tablet_id_(),
      data_transfer_seq_(-1),
      index_transfer_seq_(-1),
      start_refresh_scn_(),
      target_refresh_scn_(),
      refresh_id_(0),
      refresh_task_id_(0),
      refresh_type_(ASYNC_REFRESH_TYPE_PERIOD),
      parallelism_(OB_ASYNC_INDEX_MIN_PARALLELISM),
      schema_version_(0),
      retry_cnt_(0),
      leader_epoch_(-1),
      is_partition_local_(false),
      is_delete_index_(false),
      is_complete_refresh_(false),
      is_refresh_on_error_skip_(false),
      is_search_index_(false),
      skip_dop_quota_(false),
      sql_(nullptr),
      ls_scheduler_(nullptr)
  {}

  bool is_valid() const
  {
    return OB_INVALID_TENANT_ID != tenant_id_
        && OB_INVALID_ID != data_table_id_
        && ls_id_.is_valid()
        && data_tablet_id_.is_valid()
        && index_tablet_id_.is_valid()
        && target_refresh_scn_.is_valid()
        && is_valid_async_refresh_type(refresh_type_)
        && nullptr != sql_;
  }

  TO_STRING_KV(K_(tenant_id),
               K_(data_table_id),
               K_(ls_id),
               K_(data_tablet_id),
               K_(index_tablet_id),
               K_(data_transfer_seq),
               K_(index_transfer_seq),
               K_(start_refresh_scn),
               K_(target_refresh_scn),
               K_(refresh_id),
               K_(refresh_task_id),
               "refresh_type", async_refresh_type_to_str(refresh_type_),
               K_(parallelism),
               K_(schema_version),
               K_(retry_cnt),
               K_(is_partition_local),
               K_(is_delete_index),
               K_(is_complete_refresh),
               K_(is_search_index),
               "sql_len", (nullptr == sql_ ? 0 : sql_->length()),
               KP_(ls_scheduler));

  uint64_t tenant_id_;
  uint64_t data_table_id_;
  share::ObLSID ls_id_;                 // LS that owns the target tablet
  common::ObTabletID data_tablet_id_;   // data table tablet id (refresh source)
  common::ObTabletID index_tablet_id_;  // index table tablet id (refresh target)
  int64_t data_transfer_seq_;
  int64_t index_transfer_seq_;
  share::SCN start_refresh_scn_;        // begin SCN for this refresh
  share::SCN target_refresh_scn_;       // target SCN for this refresh
  int64_t refresh_id_;                  // monotonically increasing refresh batch id
  int64_t refresh_task_id_;             // synthetic unique id baked into the SQL hint; key for version registration
  ObAsyncRefreshType refresh_type_;
  int64_t parallelism_;                 // requested DOP passed to parallel(N)
  int64_t schema_version_;              // data table schema version captured at SQL generation time
  int64_t retry_cnt_;
  int64_t leader_epoch_;                // -1 for new tasks; retries retain their original epoch
  bool is_partition_local_;             // whether this task uses partition-local DDL execution
  bool is_delete_index_;                // whether this task targets the shared delete index
  // true => COMPLETE (full snapshot) refresh. The executor MUST run this SQL as
  // a plain snapshot scan, i.e. WITHOUT setting the session's refreshing_mview
  // flag; otherwise the base table scan is forced into mview incremental mode
  // which requires an ora_rowscn version range this SQL does not carry.
  bool is_complete_refresh_;
  bool is_refresh_on_error_skip_;       // REFRESH_ON_ERROR=SKIP 时为 true
  bool is_search_index_;               // 当前刷新目标是否为 Search Data
  bool skip_dop_quota_;                // RPC direct path: skip DOP pool admission, use parallelism=1
  const common::ObSqlString *sql_;      // pre-generated refresh SQL (referenced, not owned)
  ObLSAsyncIndexScheduler *ls_scheduler_;  // LS scheduler owning epoch/retry list (nullable)
};

// ======================== View Snapshot  ========================
// Read-only, query-time aggregate handed from ObTenantAsyncIndexRefresher to the
// virtual table iterator. Holds only value types so the snapshot has no
// lifetime dependency on scheduler / worker pointers after build returns.
struct ObAsyncIndexRefreshViewSnapshot
{
  ObAsyncIndexRefreshViewSnapshot()
    : in_flight_stats_(), retry_stats_() {}
  void reset()
  {
    in_flight_stats_.reset();
    retry_stats_.reset();
  }
  TO_STRING_KV("in_flight_cnt", in_flight_stats_.count(),
               "retry_cnt", retry_stats_.count());

  common::ObSEArray<ObRefreshTaskStat, 64> in_flight_stats_;
  common::ObSEArray<ObRefreshTaskStat, 64> retry_stats_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_DEFINE_H_

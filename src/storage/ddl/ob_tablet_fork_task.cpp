/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_tablet_fork_task.h"
#include "lib/utility/ob_tracepoint.h"
#include "logservice/ob_log_service.h"
#include "share/ob_ddl_checksum.h"
#include "share/ob_ddl_sim_point.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/access/ob_multiple_scan_merge.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_tablet_fork_util.h"
#include "storage/ddl/ob_tablet_split_util.h"
#include "storage/mds_table/ob_ls_mds_tablet_logical_reloc.h"
#include "storage/mds_table/ob_mds_table.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/compaction/ob_partition_parallel_merge_ctx.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "deps/oblib/src/lib/charset/ob_charset.h"
#include "storage/ddl/ob_pipeline.h"
#include "storage/ddl/ob_ddl_merge_task_v2.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_fork_mds_helper.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#include "storage/tablet/ob_tablet_mds_reloc_status.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/blocksstable/index_block/ob_index_block_macro_iterator.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_sstable_private_object_cleaner.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_store_row_iterator.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "src/storage/compaction_v2/ob_ss_compact_helper.h"
#include "src/storage/ddl/ob_ss_ddl_util.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/high_availability/ob_fork_table_reserved_scn_operator.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/reorganization_info_table/ob_tablet_reorg_info_table.h"
#include "storage/reorganization_info_table/ob_tablet_reorg_info_table_operation.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace compaction;
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace observer;
using namespace omt;
using namespace name;
using namespace transaction;
using namespace blocksstable;

namespace storage
{

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_FORK_TABLE_FORCE_SIMPLIFIED_DST_SCHEMA, "Force simplified dst schema in fork rewrite");
ERRSIM_POINT_DEF(EN_FORK_TABLE_BLOCK_LOCAL_LAND, "Block fork local land to simulate a slow follower");
ERRSIM_POINT_DEF(EN_FORK_MINOR_REWRITE_TASK_FAIL, "Fail fork minor rewrite task");
ERRSIM_POINT_DEF(EN_FORK_MINOR_FINISH_TASK_FAIL, "Fail fork minor finish task");
#endif

static int check_fork_mds_reloc_done_(const ObLSHandle &dest_ls_handle, const ObTabletID &tablet_id,
                                      const share::SCN &reorg_scn, bool &is_done)
{
  int ret = OB_SUCCESS;
  ObLS *dest_ls = dest_ls_handle.get_ls();
  ObMdsTable *mds_table = nullptr;
  ObTabletMdsRelocStatus reloc_status;
  is_done = false;
  if (OB_ISNULL(dest_ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] dest ls is null when checking mds reloc marker", K(ret), K(tablet_id), K(reorg_scn));
  } else if (OB_ISNULL(mds_table = dest_ls->get_mds_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] dest mds table is null when checking reloc marker", K(ret), K(tablet_id), K(reorg_scn),
        "dest_ls_id", dest_ls->get_ls_id());
  } else if (OB_FAIL(mds_table->get_latest_committed(tablet_id, reorg_scn, 5_s, reloc_status))) {
    if (OB_EMPTY_RESULT == ret || OB_ITER_END == ret || OB_SNAPSHOT_DISCARDED == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("[FORKTABLE] failed to read mds reloc marker", K(ret), K(tablet_id), K(reorg_scn),
          "dest_ls_id", dest_ls->get_ls_id());
    }
  } else {
    is_done = reloc_status.reloc_done_;
  }
  return ret;
}

#ifdef ERRSIM
static int force_simplified_fork_dst_schema_(ObIAllocator &allocator, const ObForkTableDataParam &param,
    ObStorageSchema *&storage_schema)
{
  int ret = OB_SUCCESS;
  ObStorageSchema *simplified_storage_schema = nullptr;
  if (OB_ISNULL(storage_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] unexpected null fork dst schema", K(ret), K(param));
  } else if (OB_ISNULL(simplified_storage_schema = OB_NEWx(ObStorageSchema, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[FORKTABLE] failed to allocate simplified fork dst schema", K(ret), K(param));
  } else if (OB_FAIL(simplified_storage_schema->init(allocator, *storage_schema, true /* skip_column_info */))) {
    LOG_WARN("[FORKTABLE] failed to initialize simplified fork dst schema", K(ret), K(param));
  } else {
    storage_schema = simplified_storage_schema;
    LOG_INFO("[FORKTABLE] forced simplified fork dst schema by errsim", K(param), KPC(storage_schema));
  }
  return ret;
}
#endif

static int build_fork_rowkey_read_info_(ObArenaAllocator &allocator, const ObStorageSchema &storage_schema,
    ObRowkeyReadInfo *&rowkey_read_info)
{
  int ret = OB_SUCCESS;
  int64_t full_stored_col_cnt = 0;
  ObSEArray<share::schema::ObColDesc, 16> cols_desc;
  rowkey_read_info = nullptr;
  if (OB_FAIL(storage_schema.get_mulit_version_rowkey_column_ids(cols_desc))) {
    LOG_WARN("failed to get multi version rowkey column ids", K(ret));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, rowkey_read_info))) {
    LOG_WARN("failed to alloc rowkey read info", K(ret));
  } else if (OB_FAIL(storage_schema.get_store_column_count(full_stored_col_cnt, true /* full_col */))) {
    LOG_WARN("failed to get store column count", K(ret));
  } else if (OB_FAIL(rowkey_read_info->init(allocator, full_stored_col_cnt,
                 storage_schema.get_rowkey_column_num(), storage_schema.is_oracle_mode(), cols_desc,
                 false /* is_cg_sstable */, false /* use_default_compat_version */,
                 false /* is_cs_replica_compat */))) {
    LOG_WARN("failed to init rowkey read info", K(ret));
  }
  return ret;
}

ObForkTableDataParam::ObForkTableDataParam()
  : is_inited_(false),
    orig_tenant_id_(common::OB_INVALID_TENANT_ID),
    dest_tenant_id_(common::OB_INVALID_TENANT_ID),
    orig_ls_id_(share::ObLSID::INVALID_LS_ID),
    dest_ls_id_(share::ObLSID::INVALID_LS_ID),
    orig_table_id_(common::OB_INVALID_ID),
    dest_table_id_(common::OB_INVALID_ID),
    orig_tablet_id_(ObTabletID::INVALID_TABLET_ID),
    dest_tablet_id_(ObTabletID::INVALID_TABLET_ID),
    orig_schema_version_(0),
    dest_schema_version_(0),
    snapshot_version_(0),
    task_id_(0),
    execution_id_(-1),
    compat_mode_(lib::Worker::CompatMode::INVALID),
    data_format_version_(0),
    ddl_type_(share::DDL_INVALID)
{
}

bool ObForkTableDataParam::is_valid() const
{
  return is_inited_
      && common::OB_INVALID_TENANT_ID != orig_tenant_id_
      && common::OB_INVALID_TENANT_ID != dest_tenant_id_
      && common::OB_INVALID_ID != orig_table_id_
      && common::OB_INVALID_ID != dest_table_id_
      && share::ObDDLType::DDL_FORK_TABLE == ddl_type_
      && orig_ls_id_.is_valid() && dest_ls_id_.is_valid()
      && orig_tablet_id_.is_valid() && dest_tablet_id_.is_valid()
      && snapshot_version_ > 0 && task_id_ > 0;
}

// DAG 初始化不能读取尚未在本副本 materialize 的 dst tablet。
int ObForkTableDataParam::init(const obrpc::ObDDLBuildSingleReplicaRequestArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[FORKTABLE] ObForkTableDataParam inited twice", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid() || ObDDLType::DDL_FORK_TABLE != ObDDLType(arg.ddl_type_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork param arg", K(ret), K(arg));
  } else if (OB_FAIL(ObCompatModeGetter::get_table_compat_mode(arg.tenant_id_, arg.source_table_id_, compat_mode_))) {
    LOG_WARN("[FORKTABLE] failed to get compat mode for fork param", K(ret), K(arg));
  } else {
    orig_tenant_id_ = arg.tenant_id_;
    dest_tenant_id_ = arg.dest_tenant_id_;
    orig_ls_id_ = arg.ls_id_;
    dest_ls_id_ = arg.dest_ls_id_;
    orig_table_id_ = arg.source_table_id_;
    dest_table_id_ = arg.dest_schema_id_;     // arg.dest_schema_id_ 存的是 dest table id(见 build_fork_table_dag_arg_)
    orig_tablet_id_ = arg.source_tablet_id_;
    dest_tablet_id_ = arg.dest_tablet_id_;
    orig_schema_version_ = arg.schema_version_;
    dest_schema_version_ = arg.dest_schema_version_;
    snapshot_version_ = arg.snapshot_version_;
    task_id_ = arg.task_id_;
    execution_id_ = arg.execution_id_;
    data_format_version_ = arg.data_format_version_;
    ddl_type_ = ObDDLType(arg.ddl_type_);
    is_inited_ = true;
    LOG_TRACE("[FORKTABLE] succeed to init ObForkTableDataParam (decoupled, no dst tablet read)", K(ret), KPC(this));
  }
  return ret;
}

struct ObForkMinorRewritePlan final
{
  explicit ObForkMinorRewritePlan(ObIAllocator &allocator)
    : planner_(allocator), parallel_cnt_(0), macro_seq_slot_base_(-1)
  {}
  bool is_valid() const
  {
    return planner_.is_valid() && parallel_cnt_ > 0 && parallel_cnt_ == planner_.get_concurrent_cnt()
        && macro_seq_slot_base_ >= 0;
  }

  // Per-entry range planner initialized with exactly one source table.
  ObParallelMergeCtx planner_;
  // Number of Rewrite tasks generated for this entry.
  int64_t parallel_cnt_;
  // First global MacroSeq slot reserved for this entry's data and IndexTree output.
  int64_t macro_seq_slot_base_;
  TO_STRING_KV(K_(parallel_cnt), K_(macro_seq_slot_base), K_(planner));
};

struct ObForkMinorEntry final
{
  ObForkMinorEntry() : source_table_(nullptr), need_rewrite_(false), rewrite_plan_(nullptr), output_handle_() {}
  ObForkMinorEntry(ObITable *source_table, const bool need_rewrite)
    : source_table_(source_table), need_rewrite_(need_rewrite), rewrite_plan_(nullptr), output_handle_()
  {}
  // Source Minor SSTable or MemTable represented by this entry.
  ObITable *source_table_;
  // Whether the source table must be rewritten instead of reused.
  bool need_rewrite_;
  // Per-entry range plan; null when the source table can be reused.
  ObForkMinorRewritePlan *rewrite_plan_;
  // Reused or newly created destination Minor SSTable.
  ObTableHandleV2 output_handle_;
  TO_STRING_KV(KP_(source_table), K_(need_rewrite), KP_(rewrite_plan), K_(output_handle));
};

struct ObForkActiveMinorCtx final
{
  ObForkActiveMinorCtx()
    : allocator_("ForkMinor", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()), entry_idx_(OB_INVALID_INDEX), source_(nullptr),
      end_scn_(), sstable_logic_seq_(0), data_desc_(), index_builder_(nullptr), pre_warm_param_()
  {}
  ~ObForkActiveMinorCtx() { reset(); }
  bool is_active() const { return OB_INVALID_INDEX != entry_idx_ || OB_NOT_NULL(index_builder_); }
  bool matches(const int64_t entry_idx) const
  {
    return entry_idx_ == entry_idx && OB_NOT_NULL(source_) && OB_NOT_NULL(index_builder_);
  }
  void reset()
  {
    if (OB_NOT_NULL(index_builder_)) {
      index_builder_->~ObSSTableIndexBuilder();
      allocator_.free(index_builder_);
      index_builder_ = nullptr;
    }
    data_desc_.reset();
    pre_warm_param_.reset();
    allocator_.reset();
    entry_idx_ = OB_INVALID_INDEX;
    source_ = nullptr;
    end_scn_.reset();
    sstable_logic_seq_ = 0;
  }

  // Temporary allocator for resources owned by the active entry.
  ObArenaAllocator allocator_;
  // Index of the active source table in ObForkPublishCtx::minor_entries_.
  int64_t entry_idx_;
  // Source table currently being rewritten.
  ObITable *source_;
  // Output end SCN clamped to the fork snapshot.
  share::SCN end_scn_;
  // Source Minor SSTable logic sequence preserved in the output.
  int16_t sstable_logic_seq_;
  // Shared output descriptor copied by each range writer.
  ObWholeDataStoreDesc data_desc_;
  // Shared IndexTree builder that collects all range outputs for this entry.
  ObSSTableIndexBuilder *index_builder_;
  // Destination LS and tablet pre-warm parameters used by data and index writers.
  ObPreWarmerParam pre_warm_param_;
};

struct ObForkMinorRewriteCtx final
{
  ObForkMinorRewriteCtx() : storage_schema_(nullptr), rowkey_read_info_(nullptr), active_()
  {}
  ~ObForkMinorRewriteCtx() { reset(); }
  void reset()
  {
    active_.reset();
    if (OB_NOT_NULL(rowkey_read_info_)) {
      rowkey_read_info_->~ObRowkeyReadInfo();
      rowkey_read_info_ = nullptr;
    }
    if (OB_NOT_NULL(storage_schema_)) {
      storage_schema_->~ObStorageSchema();
      storage_schema_ = nullptr;
    }
  }

  // Destination storage schema shared by all rewritten Minor entries.
  ObStorageSchema *storage_schema_;
  // Rowkey read information shared by all source range scans.
  ObRowkeyReadInfo *rowkey_read_info_;
  // Runtime state for the one Minor entry currently being rewritten.
  ObForkActiveMinorCtx active_;
};

struct ObForkPublishCtx final
{
  ObForkPublishCtx(const ObForkTableDataParam &param, ObArenaAllocator &output_allocator)
    : param_(param), output_allocator_(output_allocator), minor_rewrite_(), src_ls_handle_(),
      dst_ls_handle_(), src_tablet_handle_(), dst_tablet_handle_(), read_iter_(), minor_entries_(), tables_handle_(),
      fork_snapshot_scn_(), dst_reorg_scn_(), fork_minor_helper_()
  {}
  ~ObForkPublishCtx() { reset(); }
  void reset()
  {
    fork_minor_helper_.reset();
    for (int64_t i = 0; i < minor_entries_.count(); ++i) {
      ObForkMinorRewritePlan *&rewrite_plan = minor_entries_.at(i).rewrite_plan_;
      if (OB_NOT_NULL(rewrite_plan)) {
        rewrite_plan->~ObForkMinorRewritePlan();
        output_allocator_.free(rewrite_plan);
        rewrite_plan = nullptr;
      }
    }
    minor_rewrite_.reset();
    tables_handle_.reset();
    minor_entries_.reset();
    read_iter_.reset();
    src_tablet_handle_.reset();
    dst_tablet_handle_.reset();
    src_ls_handle_.reset();
    dst_ls_handle_.reset();
  }

  // Immutable Fork DAG parameters owned by ObForkTableDataDag.
  const ObForkTableDataParam &param_;
  // DAG-lifetime arena used by rewrite plans and output SSTables.
  ObArenaAllocator &output_allocator_;
  // Schema, rowkey, and active-entry state shared by Minor rewrite tasks.
  ObForkMinorRewriteCtx minor_rewrite_;
  // Source LS handle retained for the whole publish task graph.
  ObLSHandle src_ls_handle_;
  // Destination LS handle retained for the whole publish task graph.
  ObLSHandle dst_ls_handle_;
  // Source tablet handle used to collect and plan snapshot input tables.
  ObTabletHandle src_tablet_handle_;
  // Destination tablet handle used to build output metadata.
  ObTabletHandle dst_tablet_handle_;
  // Iterator over source tables visible at the fork snapshot.
  ObTabletTableIterator read_iter_;
  // Ordered Minor SSTable and MemTable entries selected from read_iter_.
  ObSEArray<ObForkMinorEntry, 8> minor_entries_;
  // Complete destination table set accumulated for the final shared-meta replace.
  ObTablesHandleArray tables_handle_;
  // Transaction SCN converted from param_.snapshot_version_.
  share::SCN fork_snapshot_scn_;
  // Destination tablet reorganization SCN used by shared-meta operations.
  share::SCN dst_reorg_scn_;
  // Atomic Minor protocol and global MacroSeq slot helper.
  ObForkDataSSHelper fork_minor_helper_;
};

class ObForkMinorPrepareTask final : public ObITask
{
public:
  ObForkMinorPrepareTask()
    : ObITask(TASK_TYPE_COMPLEMENT_PREPARE), is_inited_(false), ctx_(nullptr), entry_idx_(OB_INVALID_INDEX),
      wait_task_(nullptr)
  {}
  int init(ObForkPublishCtx *ctx, const int64_t entry_idx, ObITask *wait_task);
private:
  int process() override;
  // Whether init() has completed successfully.
  bool is_inited_;
  // Publish context shared by all Fork Minor tasks in the DAG.
  ObForkPublishCtx *ctx_;
  // Index of the source table in ctx_->minor_entries_.
  int64_t entry_idx_;
  // Barrier released only after this entry's Finish task completes.
  ObITask *wait_task_;
};

class ObForkMinorRewriteTask final : public ObITask
{
public:
  ObForkMinorRewriteTask()
    : ObITask(TASK_TYPE_COMPLEMENT_WRITE), is_inited_(false), ctx_(nullptr), entry_idx_(OB_INVALID_INDEX),
      task_idx_(OB_INVALID_INDEX)
  {}
  int init(ObForkPublishCtx *ctx, const int64_t entry_idx, const int64_t task_idx);
private:
  int generate_next_task(ObITask *&next_task) override;
  int process() override;
  // Whether init() has completed successfully.
  bool is_inited_;
  // Publish context shared by all Fork Minor tasks in the DAG.
  ObForkPublishCtx *ctx_;
  // Index of the source table in ctx_->minor_entries_.
  int64_t entry_idx_;
  // Index of this Rewrite task in the entry-local range plan.
  int64_t task_idx_;
};

class ObForkMinorFinishTask final : public ObITask
{
public:
  ObForkMinorFinishTask()
    : ObITask(TASK_TYPE_COMPLEMENT_MERGE), is_inited_(false), ctx_(nullptr), entry_idx_(OB_INVALID_INDEX)
  {}
  int init(ObForkPublishCtx *ctx, const int64_t entry_idx);
private:
  int process() override;
  // Whether init() has completed successfully.
  bool is_inited_;
  // Publish context shared by all Fork Minor tasks in the DAG.
  ObForkPublishCtx *ctx_;
  // Index of the source table in ctx_->minor_entries_.
  int64_t entry_idx_;
};

class ObForkTablePublishTask final : public ObITask
{
public:
  ObForkTablePublishTask() : ObITask(TASK_TYPE_COMPLEMENT_MERGE), is_inited_(false), ctx_(nullptr) {}
  int init(ObForkPublishCtx *ctx);
private:
  int process() override;
  // Whether init() has completed successfully.
  bool is_inited_;
  // Publish context containing every table to publish atomically.
  ObForkPublishCtx *ctx_;
};

ObForkTableDataDag::ObForkTableDataDag()
  : ObIDag(ObDagType::DAG_TYPE_DDL), is_inited_(false), param_(), fork_phase_(ObForkDataPhase::INVALID),
    publish_allocator_("ForkPublish", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()), publish_ctx_(nullptr)
{
}

ObForkTableDataDag::~ObForkTableDataDag()
{
  clear_task_list_with_lock();
  if (OB_NOT_NULL(publish_ctx_)) {
    publish_ctx_->~ObForkPublishCtx();
    publish_allocator_.free(publish_ctx_);
    publish_ctx_ = nullptr;
  }
  publish_allocator_.reset();
}

int ObForkTableDataDag::init(
    const ObDDLBuildSingleReplicaRequestArg &arg,
    const ObForkDataPhase fork_phase)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[FORKTABLE] ObForkTableDataDag has already been inited", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid()
      || ObDDLType::DDL_FORK_TABLE != ObDDLType(arg.ddl_type_)
      || ObForkDataPhase::INVALID == fork_phase)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork table data dag arg",
        K(ret), K(arg), "fork_phase", static_cast<int64_t>(fork_phase));
  } else if (OB_FAIL(param_.init(arg))) {
    LOG_WARN("[FORKTABLE] failed to init fork table data dag param", K(ret), K(arg));
  } else if (OB_UNLIKELY(!param_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] invalid fork table data dag param", K(ret), K(param_));
  } else {
    consumer_group_id_ = arg.consumer_group_id_;
    fork_phase_ = fork_phase;
    is_inited_ = true;
  }
  LOG_TRACE("[FORKTABLE] finish to init fork table data dag",
      K(ret), K(param_), "fork_phase", static_cast<int64_t>(fork_phase_));
  return ret;
}

int ObForkTableDataDag::create_first_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table data dag not init", K(ret));
  } else if (ObForkDataPhase::FORK_PUBLISH == fork_phase_) {
    ObForkTablePrepareTask *task = nullptr;
    if (OB_FAIL(alloc_task(task))) {
      LOG_WARN("[FORKTABLE] allocate fork table prepare task failed", K(ret));
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr task", K(ret));
    } else if (OB_FAIL(task->init(this))) {
      LOG_WARN("[FORKTABLE] init fork table prepare task failed", K(ret));
    } else if (OB_FAIL(add_task(*task))) {
      LOG_WARN("[FORKTABLE] add fork table prepare task failed", K(ret));
    }
  } else if (ObForkDataPhase::FORK_LOCAL_LAND == fork_phase_) {
    ObForkDownloadSSTableTask *download_task = nullptr;
    if (OB_FAIL(alloc_task(download_task))) {
      LOG_WARN("[FORKTABLE] allocate fork download task failed", K(ret));
    } else if (OB_ISNULL(download_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr download task", K(ret));
    } else if (OB_FAIL(download_task->init(this))) {
      LOG_WARN("[FORKTABLE] init fork download task failed", K(ret));
    } else if (OB_FAIL(add_task(*download_task))) {
      LOG_WARN("[FORKTABLE] add fork download task failed", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] invalid fork phase for create first task", K(ret), "fork_phase", static_cast<int64_t>(fork_phase_));
  }
  return ret;
}

uint64_t ObForkTableDataDag::hash() const
{
  uint64_t hash_val = 0;
  if (OB_LIKELY(is_inited_ && param_.is_valid())) {
    hash_val = param_.orig_tenant_id_ + param_.dest_tenant_id_
             + param_.orig_table_id_ + param_.dest_table_id_
             + param_.orig_ls_id_.hash() + param_.dest_ls_id_.hash()
             + param_.orig_tablet_id_.hash() + param_.dest_tablet_id_.hash()
             + param_.task_id_ + ObDagType::DAG_TYPE_DDL
             + static_cast<uint64_t>(fork_phase_);
  }
  return hash_val;
}

bool ObForkTableDataDag::operator==(const ObIDag &other) const
{
  bool is_equal = false;
  int tmp_ret = OB_SUCCESS;
  char this_key[256] = {0};
  char other_key[256] = {0};
  if (this == &other) {
    is_equal = true;
  } else if (get_type() == other.get_type()) {
    if (OB_SUCCESS != (tmp_ret = fill_dag_key(this_key, sizeof(this_key)))) {
      LOG_WARN("[FORKTABLE] failed to fill fork dag key", K(tmp_ret), K(param_));
    } else if (OB_SUCCESS != (tmp_ret = other.fill_dag_key(other_key, sizeof(other_key)))) {
      LOG_WARN("failed to fill other ddl dag key", K(tmp_ret), K(other.get_type()));
    } else {
      is_equal = (0 == STRCMP(this_key, other_key));
    }
  }
  return is_equal;
}

bool ObForkTableDataDag::ignore_warning()
{
  return OB_EAGAIN == dag_ret_
      || OB_NEED_RETRY == dag_ret_
      || OB_TASK_EXPIRED == dag_ret_;
}

int ObForkTableDataDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || !param_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table data dag has not been initialized", K(ret), K_(is_inited), K_(param));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                param_.orig_ls_id_.id(),
                                static_cast<int64_t>(param_.orig_tablet_id_.id()),
                                static_cast<int64_t>(param_.dest_tablet_id_.id()),
                                static_cast<int64_t>(param_.orig_table_id_),
                                static_cast<int64_t>(param_.dest_table_id_),
                                param_.orig_schema_version_,
                                param_.snapshot_version_))) {
    LOG_WARN("[FORKTABLE] failed to fill fork table data dag info param", K(ret));
  }
  return ret;
}

int ObForkTableDataDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || !param_.is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table data dag has not been initialized", K(ret), K_(is_inited), K_(param));
  } else if (OB_FAIL(databuff_printf(buf, buf_len,
                              "fork_table logstream_id=%ld source_tablet_id=%ld dest_tablet_id=%ld "
                              "task_id=%ld execution_id=%ld fork_phase=%ld",
                              param_.orig_ls_id_.id(), param_.orig_tablet_id_.id(),
                              param_.dest_tablet_id_.id(), param_.task_id_, param_.execution_id_,
                              static_cast<int64_t>(fork_phase_)))) {
    LOG_WARN("[FORKTABLE] fill fork table data dag key failed", K(ret), K(param_));
  }
  return ret;
}

int ObForkTableDataDag::collect_macro_ids_(
    const ObSSTable &sstable,
    ObIArray<MacroBlockId> &data_block_ids,
    ObIArray<MacroBlockId> &other_block_ids)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  ObMacroIdIterator macro_iter;
  MacroBlockId macro_id;
  data_block_ids.reset();
  other_block_ids.reset();
  if (OB_FAIL(sstable.get_meta(meta_handle))) {
    LOG_WARN("failed to get source sstable meta", K(ret), K(sstable));
  } else if (OB_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_data_block_iter(macro_iter))) {
    LOG_WARN("failed to get source data macro iterator", K(ret), K(sstable));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(macro_iter.get_next_macro_id(macro_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next source data macro id", K(ret), K(sstable));
        }
      } else if (OB_FAIL(data_block_ids.push_back(macro_id))) {
        LOG_WARN("failed to push source data macro id", K(ret), K(macro_id));
      }
    }
  }
  macro_iter.reset();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(meta_handle.get_sstable_meta().get_macro_info().get_other_block_iter(macro_iter))) {
    LOG_WARN("failed to get source other macro iterator", K(ret), K(sstable));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(macro_iter.get_next_macro_id(macro_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next source other macro id", K(ret), K(sstable));
        }
      } else if (OB_FAIL(other_block_ids.push_back(macro_id))) {
        LOG_WARN("failed to push source other macro id", K(ret), K(macro_id));
      }
    }
  }
  return ret;
}

static int check_fork_sstable_shared_(const ObSSTable &sstable, bool &is_shared)
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;
  is_shared = false;
  if (OB_FAIL(sstable.get_meta(meta_handle))) {
    LOG_WARN("[FORKTABLE] failed to get source sstable meta", K(ret), K(sstable));
  } else {
    is_shared = meta_handle.get_sstable_meta().is_shared_table();
  }
  return ret;
}

int ObForkTableDataDag::build_reused_sstable_object_(
    ObSSTable &src_sstable,
    ObArenaAllocator &allocator,
    ObTableHandleV2 &sstable_handle,
    const int64_t override_upper_trans_version)
{
  int ret = OB_SUCCESS;
  ObMigrationSSTableParam migration_param;
  ObArray<MacroBlockId> data_block_ids;
  ObArray<MacroBlockId> other_block_ids;
  ObTabletCreateSSTableParam create_sstable_param;
  if (OB_FAIL(ObTablet::build_migration_sstable_param(src_sstable, migration_param))) {
    LOG_WARN("[FORKTABLE] failed to build migration sstable param for fork reuse", K(ret), K(src_sstable));
  } else if (OB_FAIL(collect_macro_ids_(src_sstable, data_block_ids, other_block_ids))) {
    LOG_WARN("failed to collect source macro ids", K(ret), K(src_sstable));
  } else {
    // The base Major must keep the source snapshot so later delta Minors are not treated as covered.
    migration_param.table_key_.tablet_id_ = param_.dest_tablet_id_;
    migration_param.basic_meta_.schema_version_ = param_.dest_schema_version_;
    if (INT64_MAX != override_upper_trans_version) {
      // Persist the resolved commit version in the destination clone so local land does not
      // treat INT64_MAX as newer than S.
      migration_param.basic_meta_.set_upper_trans_version(override_upper_trans_version);
    }
    if (OB_FAIL(create_sstable_param.init_for_ha(migration_param, data_block_ids, other_block_ids))) {
      LOG_WARN("[FORKTABLE] failed to init fork reused sstable param", K(ret), K(migration_param));
    } else if (src_sstable.is_co_sstable()
        && OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObCOSSTableV2>(
            create_sstable_param, allocator, sstable_handle))) {
      LOG_WARN("[FORKTABLE] failed to create fork reused co sstable", K(ret), K(create_sstable_param));
    } else if (!src_sstable.is_co_sstable()
        && OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(
            create_sstable_param, allocator, sstable_handle))) {
      LOG_WARN("[FORKTABLE] failed to create fork reused sstable", K(ret), K(create_sstable_param));
    }
  }
  return ret;
}

int ObForkTableDataDag::build_reused_co_sstable_(ObSSTable &src_sstable, ObArenaAllocator &allocator,
    ObTableHandleV2 &sstable_handle, const int64_t override_upper_trans_version)
{
  int ret = OB_SUCCESS;
  ObCOSSTableV2 *src_co_sstable = nullptr;
  ObSSTable *dst_sstable = nullptr;
  ObArray<ObSSTableWrapper> src_tables;
  ObTablesHandleArray dst_cg_handles;
  ObArray<ObITable *> dst_cg_tables;
  ObTableHandleV2 dst_co_handle;
  // Reuse one handle wrapper across CO/CG clones. Retained objects are copied to the parent or CG handle arrays.
  ObTableHandleV2 dst_handle;
  if (OB_UNLIKELY(!src_sstable.is_co_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] source sstable is not a co sstable", K(ret), K(src_sstable));
  } else if (OB_FALSE_IT(src_co_sstable = static_cast<ObCOSSTableV2 *>(&src_sstable))) {
  } else if (OB_FAIL(src_co_sstable->get_all_tables(src_tables))) {
    LOG_WARN("[FORKTABLE] failed to get source co and cg sstables", K(ret), K(src_sstable));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < src_tables.count(); ++i) {
    ObSSTable *src_table = nullptr;
    dst_handle.reset();
    if (OB_FAIL(src_tables.at(i).get_loaded_column_store_sstable(src_table))) {
      LOG_WARN("[FORKTABLE] failed to load source co or cg sstable", K(ret), K(i), K(src_tables.at(i)));
    } else if (OB_ISNULL(src_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] source co or cg sstable is null", K(ret), K(i));
    } else if (OB_FAIL(build_reused_sstable_object_(
                   *src_table, allocator, dst_handle, override_upper_trans_version))) {
      LOG_WARN("[FORKTABLE] failed to clone source co or cg sstable", K(ret), K(i), KPC(src_table));
    } else if (src_table == src_co_sstable) {
      dst_co_handle = dst_handle;
    } else if (OB_FAIL(dst_cg_handles.add_table(dst_handle))) {
      LOG_WARN("[FORKTABLE] failed to retain cloned cg sstable", K(ret), K(i), K(dst_handle));
    } else if (OB_FAIL(dst_cg_tables.push_back(dst_handle.get_table()))) {
      LOG_WARN("[FORKTABLE] failed to collect cloned cg sstable", K(ret), K(i), K(dst_handle));
    }
  }
  if (FAILEDx(dst_co_handle.get_sstable(dst_sstable))) {
    LOG_WARN("[FORKTABLE] failed to get cloned co sstable", K(ret), K(dst_co_handle));
  } else if (OB_ISNULL(dst_sstable) || OB_UNLIKELY(!dst_sstable->is_co_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] cloned co sstable is invalid", K(ret), KPC(dst_sstable));
  } else if (!src_co_sstable->is_cgs_empty_co_table()
      && OB_FAIL(static_cast<ObCOSSTableV2 *>(dst_sstable)->fill_cg_sstables(dst_cg_tables))) {
    LOG_WARN("[FORKTABLE] failed to assemble cloned co sstable", K(ret), K(dst_cg_tables));
  } else {
    sstable_handle = dst_co_handle;
  }
  return ret;
}

int ObForkTableDataDag::build_reused_sstable_(ObSSTable &src_sstable, ObArenaAllocator &allocator,
    ObTableHandleV2 &sstable_handle, const int64_t override_upper_trans_version)
{
  int ret = OB_SUCCESS;
  if (src_sstable.is_co_sstable() && OB_FAIL(build_reused_co_sstable_(src_sstable, allocator, sstable_handle, override_upper_trans_version))) {
    LOG_WARN("[FORKTABLE] failed to build fork reused co sstable", K(ret), K(src_sstable));
  } else if (!src_sstable.is_co_sstable() && OB_FAIL(build_reused_sstable_object_(src_sstable, allocator, sstable_handle, override_upper_trans_version))) {
    LOG_WARN("[FORKTABLE] failed to build fork reused sstable", K(ret), K(src_sstable));
  } else {
    LOG_INFO("[FORKTABLE] created fork reused sstable", K(param_), "is_major", src_sstable.is_major_sstable(),
        "is_co", src_sstable.is_co_sstable(), "table_key", sstable_handle.get_table()->get_key());
  }
  return ret;
}

// An INC_MAJOR enters the destination only when committed at the fork snapshot with commit version no greater than S.
// Query the transaction state and commit version when upper_trans_version has not been filled.
int ObForkTableDataDag::resolve_fork_inc_major_commit_version_(
    ObSSTable &inc_major,
    bool &is_visible,
    int64_t &commit_version)
{
  int ret = OB_SUCCESS;
  is_visible = false;
  const int64_t upper_trans_version = inc_major.get_upper_trans_version();
  commit_version = upper_trans_version;
  if (INT64_MAX != upper_trans_version) {
    is_visible = upper_trans_version <= param_.snapshot_version_;
  } else {
    ObLSService *ls_service = MTL(ObLSService *);
    ObLSHandle ls_handle;
    int64_t trans_state = ObTxData::RUNNING;
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null", K(ret));
    } else if (OB_FAIL(ls_service->get_ls(param_.orig_ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("failed to get source ls for inc major commit version", K(ret), K(param_));
    } else if (OB_ISNULL(ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("source ls is null", K(ret), K(param_));
    } else if (OB_FAIL(compaction::ObIncMajorTxHelper::get_inc_major_commit_version(
                   *ls_handle.get_ls(), inc_major, share::SCN::max_scn(), trans_state, commit_version))) {
      LOG_WARN("failed to get inc major commit version", K(ret), K(inc_major));
    } else if (ObTxData::COMMIT == trans_state && OB_UNLIKELY(commit_version <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] invalid inc major commit version", K(ret), K(commit_version), K(inc_major));
    } else if (ObTxData::COMMIT == trans_state) {
      is_visible = commit_version <= param_.snapshot_version_;
    } else {
      is_visible = false;
    }
    LOG_INFO("[FORKTABLE] resolve inc major commit version by tx", K(ret), K(trans_state),
        K(commit_version), K(is_visible), "fork_snapshot", param_.snapshot_version_,
        "table_key", inc_major.get_key());
  }
  return ret;
}

int ObForkTableDataDag::init_fork_publish_ctx_(ObArenaAllocator &allocator, ObForkPublishCtx *&ctx)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObTablet *src_tablet = nullptr;
  ctx = nullptr;
  if (OB_NOT_NULL(publish_ctx_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[FORKTABLE] fork publish context initialized twice", K(ret), K(param_));
  } else if (OB_ISNULL(ctx = OB_NEWx(ObForkPublishCtx, &allocator, param_, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[FORKTABLE] failed to allocate fork publish context", K(ret), K(param_));
  } else if (FALSE_IT(publish_ctx_ = ctx)) {
  } else if (OB_FAIL(ctx->fork_snapshot_scn_.convert_for_tx(param_.snapshot_version_))) {
    LOG_WARN("[FORKTABLE] failed to convert fork snapshot scn", K(ret), K(param_));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(param_.orig_ls_id_, ctx->src_ls_handle_, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get source ls", K(ret), K(param_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ctx->src_ls_handle_, param_.orig_tablet_id_,
                 ctx->src_tablet_handle_, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get source tablet", K(ret), K(param_));
  } else if (OB_ISNULL(src_tablet = ctx->src_tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source tablet is null", K(ret), K(ctx->src_tablet_handle_));
  } else if (OB_FAIL(ls_service->get_ls(param_.dest_ls_id_, ctx->dst_ls_handle_, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get dest ls", K(ret), K(param_));
  } else if (OB_ISNULL(ctx->dst_ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest ls is null", K(ret), K(param_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ctx->dst_ls_handle_, param_.dest_tablet_id_,
                 ctx->dst_tablet_handle_, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get dest tablet", K(ret), K(param_));
  } else if (OB_FAIL(src_tablet->get_fork_snapshot_read_tables(param_.snapshot_version_, ctx->read_iter_))) {
    LOG_WARN("[FORKTABLE] failed to get fork snapshot read tables", K(ret), K(param_));
  } else if (OB_ISNULL(ctx->read_iter_.table_iter())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] fork read table iterator is null", K(ret), K(param_));
  } else {
    ctx->dst_reorg_scn_ = ctx->dst_tablet_handle_.get_obj()->get_reorganization_scn();
  }
  return ret;
}

int ObForkTableDataDag::prepare_fork_publish_inc_major_(ObSSTable &inc_major, const int64_t table_idx,
    ObArenaAllocator &allocator, ObForkPublishCtx &ctx)
{
  int ret = OB_SUCCESS;
  bool is_visible = false;
  bool is_shared = false;
  int64_t commit_version = INT64_MAX;
  ObTableHandleV2 dst_handle;
  if (OB_FAIL(resolve_fork_inc_major_commit_version_(inc_major, is_visible, commit_version))) {
    LOG_WARN("[FORKTABLE] failed to resolve fork inc major commit version", K(ret), "i", table_idx, K(inc_major));
  } else if (!is_visible) {
    LOG_INFO("[FORKTABLE] skip fork source inc major invisible at fork snapshot", "i", table_idx,
        "table_key", inc_major.get_key(), "upper_trans_version", inc_major.get_upper_trans_version(),
        "commit_version", commit_version, "fork_snapshot", param_.snapshot_version_);
  } else if (OB_FAIL(check_fork_sstable_shared_(inc_major, is_shared))) {
    LOG_WARN("[FORKTABLE] failed to check fork source inc major shared state", K(ret), "i", table_idx, K(inc_major));
  } else if (!is_shared) {
    ret = OB_EAGAIN;
    LOG_INFO("[FORKTABLE] fork source inc major not shared yet, retry dag later", K(ret), "i", table_idx, K(inc_major));
  } else if (OB_FAIL(build_reused_sstable_(inc_major, allocator, dst_handle, commit_version))) {
    LOG_WARN("[FORKTABLE] failed to build fork reused inc major", K(ret), "i", table_idx, K(inc_major));
  } else if (OB_FAIL(ctx.tables_handle_.add_table(dst_handle))) {
    LOG_WARN("[FORKTABLE] failed to add fork dst inc major", K(ret), "i", table_idx);
  }
  return ret;
}

int ObForkTableDataDag::build_shared_empty_major_(ObSSTable &src_major, ObArenaAllocator &allocator,
    ObForkPublishCtx &ctx, ObTableHandleV2 &sstable_handle)
{
  int ret = OB_SUCCESS;
  ObStorageSchema *storage_schema = nullptr;
  if (OB_FAIL(ctx.dst_tablet_handle_.get_obj()->load_storage_schema(allocator, storage_schema))) {
    LOG_WARN("failed to load dest storage schema for fork empty Major", K(ret), K(param_));
  } else if (src_major.is_co_sstable()
      && OB_FAIL(ObTabletCreateDeleteHelper::create_shared_empty_co_sstable(allocator, *storage_schema,
          param_.dest_tablet_id_, src_major.get_snapshot_version(), sstable_handle))) {
    LOG_WARN("[FORKTABLE] failed to create fork shared empty CO Major", K(ret), K(param_), K(src_major));
  } else if (!src_major.is_co_sstable()
      && OB_FAIL(ObTabletCreateDeleteHelper::create_shared_empty_sstable(allocator, *storage_schema,
          param_.dest_tablet_id_, src_major.get_snapshot_version(), sstable_handle))) {
    LOG_WARN("[FORKTABLE] failed to create fork shared empty Major", K(ret), K(param_), K(src_major));
  } else {
    LOG_INFO("[FORKTABLE] created fork shared empty Major", K(param_), "src_table_key", src_major.get_key());
  }
  return ret;
}

int ObForkTableDataDag::prepare_fork_publish_major_(ObSSTable &major, const int64_t table_idx,
    ObArenaAllocator &allocator, ObForkPublishCtx &ctx, bool &has_major)
{
  int ret = OB_SUCCESS;
  bool is_shared = false;
  ObTableHandleV2 dst_handle;
  if (OB_FAIL(check_fork_sstable_shared_(major, is_shared))) {
    LOG_WARN("[FORKTABLE] failed to check fork source major shared state", K(ret), "i", table_idx, K(major));
  }
  if (OB_FAIL(ret)) {
  } else if (is_shared && OB_FAIL(build_reused_sstable_(major, allocator, dst_handle))) {
    LOG_WARN("[FORKTABLE] failed to build fork reused major", K(ret), "i", table_idx, K(major));
  } else if (!is_shared && OB_UNLIKELY(!major.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("[FORKTABLE] private fork source Major is not empty", K(ret), "i", table_idx, K(major));
  } else if (!is_shared && OB_FAIL(build_shared_empty_major_(major, allocator, ctx, dst_handle))) {
    LOG_WARN("[FORKTABLE] failed to build fork shared empty Major", K(ret), "i", table_idx, K(major));
  } else if (OB_FAIL(ctx.tables_handle_.add_table(dst_handle))) {
    LOG_WARN("[FORKTABLE] failed to add fork dst major", K(ret), "i", table_idx);
  } else {
    has_major = true;
  }
  return ret;
}

int ObForkTableDataDag::prepare_fork_publish_minor_(
    ObITable *table, const int64_t table_idx, ObArenaAllocator &allocator, ObForkPublishCtx &ctx)
{
  int ret = OB_SUCCESS;
  bool can_reuse = false;
  if (table->is_minor_sstable() && table->get_end_scn() <= ctx.fork_snapshot_scn_
      && OB_FAIL(check_fork_sstable_shared_(*static_cast<ObSSTable *>(table), can_reuse))) {
    LOG_WARN("[FORKTABLE] failed to check fork source minor shared state", K(ret), "i", table_idx, KPC(table));
  }
  ObForkMinorEntry entry(table, !can_reuse);
  if (OB_FAIL(ret)) {
  } else if (can_reuse
      && OB_FAIL(build_reused_sstable_(*static_cast<ObSSTable *>(table), allocator, entry.output_handle_))) {
    LOG_WARN("[FORKTABLE] failed to build fork reused minor", K(ret), "i", table_idx, KPC(table));
  } else if (OB_FAIL(ctx.minor_entries_.push_back(entry))) {
    LOG_WARN("[FORKTABLE] failed to collect fork minor entry", K(ret), "i", table_idx);
  }
  return ret;
}

int ObForkTableDataDag::collect_fork_publish_tables_(ObArenaAllocator &allocator, ObForkPublishCtx &ctx)
{
  int ret = OB_SUCCESS;
  bool has_major = false;
  ObTableStoreIterator *store_iter = ctx.read_iter_.table_iter();
  for (int64_t i = 0; OB_SUCC(ret) && i < store_iter->count(); ++i) {
    ObITable *table = nullptr;
    if (OB_FAIL(store_iter->get_ith_table(i, table))) {
      LOG_WARN("failed to get ith source table", K(ret), K(i));
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("source table is null", K(ret), K(i));
    } else if (table->is_inc_major_type_sstable()) {
      ret = prepare_fork_publish_inc_major_(*static_cast<ObSSTable *>(table), i, allocator, ctx);
    } else if (table->is_major_sstable()) {
      ret = prepare_fork_publish_major_(*static_cast<ObSSTable *>(table), i, allocator, ctx, has_major);
    } else if (!(table->is_minor_sstable() || table->is_memtable())) {
      continue;
    } else if (table->get_start_scn() >= ctx.fork_snapshot_scn_) {
      LOG_INFO("[FORKTABLE] skip fork source minor at or beyond fork snapshot", K(i), K(ctx.fork_snapshot_scn_),
          "start_scn", table->get_start_scn(), "end_scn", table->get_end_scn(), "is_memtable", table->is_memtable());
      continue;
    } else {
      ret = prepare_fork_publish_minor_(table, i, allocator, ctx);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!has_major)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] fork dst has no base major to publish", K(ret), K(param_));
  }
  return ret;
}

static bool has_fork_minor_rewrite_(const ObForkPublishCtx &ctx)
{
  bool has_rewrite = false;
  for (int64_t i = 0; !has_rewrite && i < ctx.minor_entries_.count(); ++i) {
    has_rewrite = ctx.minor_entries_.at(i).need_rewrite_;
  }
  return has_rewrite;
}

static int init_fork_minor_rewrite_plans_(ObArenaAllocator &allocator, const int64_t dag_priority,
    ObForkPublishCtx &ctx, int64_t &total_macro_seq_slot_cnt)
{
  int ret = OB_SUCCESS;
  bool enable_parallel_minor_merge = false;
  total_macro_seq_slot_cnt = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    enable_parallel_minor_merge = tenant_config->_enable_parallel_minor_merge;
  }
  ObForkMinorRewriteCtx &rewrite_ctx = ctx.minor_rewrite_;
  if (OB_FAIL(ctx.dst_tablet_handle_.get_obj()->load_storage_schema(allocator, rewrite_ctx.storage_schema_))) {
    LOG_WARN("failed to load dest storage schema for fork minor", K(ret), K(ctx.param_));
#ifdef ERRSIM
  } else if (OB_UNLIKELY(EN_FORK_TABLE_FORCE_SIMPLIFIED_DST_SCHEMA)
      && OB_FAIL(force_simplified_fork_dst_schema_(allocator, ctx.param_, rewrite_ctx.storage_schema_))) {
    LOG_WARN("[FORKTABLE] failed to force simplified fork dst schema", K(ret), K(ctx.param_));
#endif
  } else if (OB_FAIL(build_fork_rowkey_read_info_(
                 allocator, *rewrite_ctx.storage_schema_, rewrite_ctx.rowkey_read_info_))) {
    LOG_WARN("[FORKTABLE] failed to build fork minor rowkey read info", K(ret), K(ctx.param_));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.minor_entries_.count(); ++i) {
    ObForkMinorEntry &entry = ctx.minor_entries_.at(i);
    if (!entry.need_rewrite_) {
      continue;
    }
    ObForkMinorRewritePlan *rewrite_plan = nullptr;
    ObSEArray<ObITable *, 1> rewrite_table;
    if (OB_FAIL(rewrite_table.push_back(entry.source_table_))) {
      LOG_WARN("[FORKTABLE] failed to collect one fork minor rewrite table", K(ret), K(i));
    } else if (OB_ISNULL(rewrite_plan = OB_NEWx(ObForkMinorRewritePlan, &allocator, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[FORKTABLE] failed to allocate fork minor rewrite plan", K(ret), K(i));
    } else if (OB_FAIL(rewrite_plan->planner_.init_parallel_fork_minor_rewrite(
                   ctx.src_tablet_handle_.get_obj()->get_rowkey_read_info(), rewrite_table,
                   rewrite_ctx.storage_schema_->get_tablet_size(), dag_priority, enable_parallel_minor_merge))) {
      LOG_WARN("[FORKTABLE] failed to plan one fork minor rewrite", K(ret), K(i), K(ctx.param_));
    } else if (FALSE_IT(rewrite_plan->parallel_cnt_ = rewrite_plan->planner_.get_concurrent_cnt())) {
    } else if (OB_UNLIKELY(rewrite_plan->parallel_cnt_ < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] invalid fork minor rewrite parallel count", K(ret), K(i), KPC(rewrite_plan));
    } else if (OB_UNLIKELY(INT64_MAX == rewrite_plan->parallel_cnt_
        || total_macro_seq_slot_cnt > INT64_MAX - (rewrite_plan->parallel_cnt_ + 1))) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("[FORKTABLE] fork minor macro sequence slot count overflow", K(ret), K(i),
          K(total_macro_seq_slot_cnt), KPC(rewrite_plan));
    } else {
      rewrite_plan->macro_seq_slot_base_ = total_macro_seq_slot_cnt;
      total_macro_seq_slot_cnt += rewrite_plan->parallel_cnt_ + 1;
      entry.rewrite_plan_ = rewrite_plan;
      rewrite_plan = nullptr;
      LOG_INFO("[FORKTABLE] planned one fork minor rewrite", K(i),
          "source_table_type", static_cast<int64_t>(entry.source_table_->get_table_type()),
          "parallel_cnt", entry.rewrite_plan_->parallel_cnt_,
          "macro_seq_slot_base", entry.rewrite_plan_->macro_seq_slot_base_,
          "entry_last_slot", total_macro_seq_slot_cnt - 1, "tablet_id", ctx.param_.dest_tablet_id_);
    }
    if (OB_NOT_NULL(rewrite_plan)) {
      rewrite_plan->~ObForkMinorRewritePlan();
      allocator.free(rewrite_plan);
      rewrite_plan = nullptr;
    }
  }
  return ret;
}

static int start_fork_minor_add_op_(ObForkPublishCtx &ctx, const int64_t total_macro_seq_slot_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ctx.fork_minor_helper_.start_add_minor_op(ctx.dst_ls_handle_.get_ls()->get_ls_id(),
                 ctx.fork_snapshot_scn_, ctx.dst_reorg_scn_, total_macro_seq_slot_cnt,
                 ctx.param_.dest_tablet_id_))) {
    LOG_WARN("[FORKTABLE] failed to start fork minor add op", K(ret), K(total_macro_seq_slot_cnt), K(ctx.param_));
  }
  return ret;
}

// Build the static backbone below for entries that require rewrite. Each MinorPrepare task dynamically creates
// parallel Rewrite tasks and a Finish task between itself and the corresponding MinorWait task.
//
// ForkPrepare
//      |
//      v
// MinorPrepare[0] --> Rewrite[0][0..N-1] --> Finish[0] --> MinorWait[0]
//                                                               |
//                                                               v
// MinorPrepare[1] --> Rewrite[1][0..M-1] --> Finish[1] --> MinorWait[1]
//                                                               |
//                                                              ...
//                                                               |
//                                                               v
//                                                          ForkPublish
//
// With no rewrite entry, ForkPrepare directly precedes ForkPublish.
static int build_fork_minor_publish_task_graph_(ObForkTableDataDag &dag, ObForkPublishCtx &ctx,
    ObITask &fork_prepare_task)
{
  int ret = OB_SUCCESS;
  ObForkTablePublishTask *fork_publish_task = nullptr;
  ObSEArray<ObForkMinorPrepareTask *, 8> minor_prepare_tasks;
  ObSEArray<ObFakeTask *, 8> minor_wait_tasks;
  ObSEArray<ObITask *, 16> fork_subgraph_tasks;
  if (OB_FAIL(dag.alloc_task(fork_publish_task))) {
    LOG_WARN("[FORKTABLE] failed to allocate fork publish task", K(ret));
  } else if (OB_FAIL(fork_publish_task->init(&ctx))) {
    LOG_WARN("[FORKTABLE] failed to init fork publish task", K(ret));
  } else if (OB_FAIL(fork_subgraph_tasks.push_back(fork_publish_task))) {
    LOG_WARN("[FORKTABLE] failed to collect fork publish task", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx.minor_entries_.count(); ++i) {
    if (!ctx.minor_entries_.at(i).need_rewrite_) {
      continue;
    }
    ObForkMinorPrepareTask *minor_prepare_task = nullptr;
    ObFakeTask *minor_wait_task = nullptr;
    if (OB_FAIL(dag.alloc_task(minor_prepare_task))) {
      LOG_WARN("[FORKTABLE] failed to allocate fork minor prepare task", K(ret), K(i));
    } else if (OB_FAIL(dag.alloc_task(minor_wait_task))) {
      LOG_WARN("[FORKTABLE] failed to allocate fork minor wait task", K(ret), K(i));
    } else if (OB_FAIL(minor_prepare_task->init(&ctx, i, minor_wait_task))) {
      LOG_WARN("[FORKTABLE] failed to init fork minor prepare task", K(ret), K(i));
    } else if (OB_FAIL(minor_prepare_tasks.push_back(minor_prepare_task))) {
      LOG_WARN("[FORKTABLE] failed to collect fork minor prepare task", K(ret), K(i));
    } else if (OB_FAIL(minor_wait_tasks.push_back(minor_wait_task))) {
      LOG_WARN("[FORKTABLE] failed to collect fork minor wait task", K(ret), K(i));
    } else if (OB_FAIL(fork_subgraph_tasks.push_back(minor_prepare_task))) {
      LOG_WARN("[FORKTABLE] failed to collect fork minor prepare task", K(ret), K(i));
    } else if (OB_FAIL(fork_subgraph_tasks.push_back(minor_wait_task))) {
      LOG_WARN("[FORKTABLE] failed to collect fork minor wait task", K(ret), K(i));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < minor_prepare_tasks.count(); ++i) {
    ObITask *fork_successor_task = fork_publish_task;
    if (i + 1 < minor_prepare_tasks.count()) {
      fork_successor_task = minor_prepare_tasks.at(i + 1);
    }
    if (OB_FAIL(minor_prepare_tasks.at(i)->add_child(*minor_wait_tasks.at(i)))) {
      LOG_WARN("[FORKTABLE] failed to add static fork minor wait dependency", K(ret), K(i));
    } else if (OB_FAIL(minor_wait_tasks.at(i)->add_child(*fork_successor_task))) {
      LOG_WARN("[FORKTABLE] failed to add next fork minor dependency", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret)) {
    if (minor_prepare_tasks.empty()) {
      ret = fork_prepare_task.add_child(*fork_publish_task);
    } else {
      ret = fork_prepare_task.add_child(*minor_prepare_tasks.at(0));
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("[FORKTABLE] failed to anchor fork publish subgraph", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(dag.batch_add_task(fork_subgraph_tasks))) {
    LOG_WARN("[FORKTABLE] failed to activate fork publish subgraph", K(ret));
  }
  return ret;
}

// Collect all inputs visible at the fork snapshot, keep Major processing synchronous,
// and build successor tasks for Minor rewrite and Publish.
int ObForkTableDataDag::generate_fork_publish_tasks_(ObArenaAllocator &allocator, ObITask &prepare_task)
{
  int ret = OB_SUCCESS;
  int64_t total_macro_seq_slot_cnt = 0;
  ObForkPublishCtx *ctx = nullptr;
  if (OB_FAIL(init_fork_publish_ctx_(allocator, ctx))) {
    LOG_WARN("[FORKTABLE] failed to init fork publish context", K(ret), K(param_));
  } else if (OB_FAIL(collect_fork_publish_tables_(allocator, *ctx))) {
    LOG_WARN("[FORKTABLE] failed to collect fork publish tables", K(ret), K(param_));
  } else if (has_fork_minor_rewrite_(*ctx)
      && OB_FAIL(init_fork_minor_rewrite_plans_(allocator, get_priority(), *ctx, total_macro_seq_slot_cnt))) {
    LOG_WARN("[FORKTABLE] failed to init fork minor rewrite plans", K(ret), K(total_macro_seq_slot_cnt), K(param_));
  } else if (total_macro_seq_slot_cnt > 0 && OB_FAIL(start_fork_minor_add_op_(*ctx, total_macro_seq_slot_cnt))) {
    LOG_WARN("[FORKTABLE] failed to start fork minor add op", K(ret), K(total_macro_seq_slot_cnt), K(param_));
  } else if (OB_FAIL(build_fork_minor_publish_task_graph_(*this, *ctx, prepare_task))) {
    LOG_WARN("[FORKTABLE] failed to build fork minor publish task graph", K(ret), K(param_));
  }
  return ret;
}

int ObForkTableDataDag::normalize_fork_minor_snapshot_row_(
    const int64_t schema_rowkey_count,
    ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(schema_rowkey_count <= 0 || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid fork minor snapshot row", K(ret), K(schema_rowkey_count), K(row));
  } else {
    if (row.is_shadow_row()) {
      const int64_t sql_sequence_idx =
          ObMultiVersionRowkeyHelpper::get_sql_sequence_col_store_index(
              schema_rowkey_count, true/*is_multi_version*/);
      if (OB_UNLIKELY(sql_sequence_idx < 0 || sql_sequence_idx >= row.get_column_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fork minor sql sequence index is out of range",
            K(ret), K(schema_rowkey_count), K(sql_sequence_idx), K(row));
      } else {
        row.mvcc_row_flag_.set_shadow_row(false);
        row.storage_datums_[sql_sequence_idx].set_int(0);
      }
    }
    if (OB_SUCC(ret)) {
      row.set_first_multi_version_row();
      row.set_last_multi_version_row();
    }
  }
  return ret;
}

static int get_fork_minor_rewrite_plan_(ObForkPublishCtx *ctx, const int64_t entry_idx,
    ObForkMinorEntry *&entry, ObForkMinorRewritePlan *&rewrite_plan)
{
  int ret = OB_SUCCESS;
  entry = nullptr;
  rewrite_plan = nullptr;
  if (OB_ISNULL(ctx) || entry_idx < 0 || entry_idx >= ctx->minor_entries_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork minor rewrite entry", K(ret), KP(ctx), K(entry_idx));
  } else {
    entry = &ctx->minor_entries_.at(entry_idx);
    rewrite_plan = entry->rewrite_plan_;
    if (!entry->need_rewrite_ || OB_ISNULL(entry->source_table_)
        || OB_ISNULL(rewrite_plan) || !rewrite_plan->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] invalid fork minor rewrite plan", K(ret), K(entry_idx), KPC(entry), KPC(rewrite_plan));
    }
  }
  return ret;
}

int ObForkMinorPrepareTask::init(ObForkPublishCtx *ctx, const int64_t entry_idx, ObITask *wait_task)
{
  int ret = OB_SUCCESS;
  ObForkMinorEntry *entry = nullptr;
  ObForkMinorRewritePlan *rewrite_plan = nullptr;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[FORKTABLE] fork minor prepare task initialized twice", K(ret));
  } else if (OB_ISNULL(wait_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork minor prepare task argument", K(ret), KP(ctx), K(entry_idx), KP(wait_task));
  } else if (OB_FAIL(get_fork_minor_rewrite_plan_(ctx, entry_idx, entry, rewrite_plan))) {
    LOG_WARN("[FORKTABLE] failed to get fork minor prepare plan", K(ret), KP(ctx), K(entry_idx));
  } else {
    ctx_ = ctx;
    entry_idx_ = entry_idx;
    wait_task_ = wait_task;
    is_inited_ = true;
  }
  return ret;
}

int ObForkMinorRewriteTask::init(ObForkPublishCtx *ctx, const int64_t entry_idx, const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  ObForkMinorEntry *entry = nullptr;
  ObForkMinorRewritePlan *rewrite_plan = nullptr;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[FORKTABLE] fork minor rewrite task initialized twice", K(ret));
  } else if (OB_FAIL(get_fork_minor_rewrite_plan_(ctx, entry_idx, entry, rewrite_plan))) {
    LOG_WARN("[FORKTABLE] failed to get fork minor rewrite plan", K(ret), KP(ctx), K(entry_idx), K(task_idx));
  } else if (task_idx < 0 || task_idx >= rewrite_plan->parallel_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork minor rewrite task argument", K(ret), KP(ctx), K(entry_idx), K(task_idx));
  } else {
    ctx_ = ctx;
    entry_idx_ = entry_idx;
    task_idx_ = task_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObForkMinorFinishTask::init(ObForkPublishCtx *ctx, const int64_t entry_idx)
{
  int ret = OB_SUCCESS;
  ObForkMinorEntry *entry = nullptr;
  ObForkMinorRewritePlan *rewrite_plan = nullptr;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[FORKTABLE] fork minor finish task initialized twice", K(ret));
  } else if (OB_FAIL(get_fork_minor_rewrite_plan_(ctx, entry_idx, entry, rewrite_plan))) {
    LOG_WARN("[FORKTABLE] failed to get fork minor finish plan", K(ret), KP(ctx), K(entry_idx));
  } else {
    ctx_ = ctx;
    entry_idx_ = entry_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObForkTablePublishTask::init(ObForkPublishCtx *ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[FORKTABLE] fork publish task initialized twice", K(ret));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork publish task context", K(ret));
  } else {
    ctx_ = ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObForkMinorPrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObForkMinorFinishTask *finish_task = nullptr;
  ObForkMinorRewriteTask *rewrite_task = nullptr;
  ObForkMinorEntry *entry = nullptr;
  ObForkMinorRewritePlan *rewrite_plan = nullptr;
  ObSSTableMetaHandle source_meta_handle;
  int64_t minor_op_id = 0;
  int32_t transfer_epoch = -1;
  if (!is_inited_ || OB_ISNULL(ctx_) || OB_ISNULL(wait_task_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork minor prepare task not initialized", K(ret));
  } else if (OB_FAIL(get_fork_minor_rewrite_plan_(ctx_, entry_idx_, entry, rewrite_plan))) {
    LOG_WARN("[FORKTABLE] failed to get fork minor rewrite plan", K(ret), K(entry_idx_));
  } else if (ctx_->minor_rewrite_.active_.is_active()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] previous fork minor state is still active", K(ret),
        K(ctx_->minor_rewrite_.active_.entry_idx_));
  } else if (entry->source_table_->is_minor_sstable()
      && OB_FAIL(static_cast<ObSSTable *>(entry->source_table_)->get_meta(source_meta_handle))) {
    LOG_WARN("[FORKTABLE] failed to get fork minor source meta", K(ret), K(entry_idx_));
  } else if (OB_FAIL(ctx_->dst_tablet_handle_.get_obj()->get_private_transfer_epoch(transfer_epoch))) {
    LOG_WARN("[FORKTABLE] failed to get fork destination transfer epoch", K(ret), K(entry_idx_));
  } else if (OB_FAIL(ctx_->fork_minor_helper_.get_op_id(minor_op_id))) {
    LOG_WARN("[FORKTABLE] failed to get fork minor op id", K(ret), K(entry_idx_));
  } else {
    ObForkMinorRewriteCtx &rewrite_ctx = ctx_->minor_rewrite_;
    ObForkActiveMinorCtx &active_ctx = rewrite_ctx.active_;
    active_ctx.entry_idx_ = entry_idx_;
    active_ctx.source_ = entry->source_table_;
    active_ctx.end_scn_ = entry->source_table_->get_end_scn();
    if (active_ctx.end_scn_ > ctx_->fork_snapshot_scn_) {
      active_ctx.end_scn_ = ctx_->fork_snapshot_scn_;
    }
    if (entry->source_table_->is_minor_sstable()) {
      active_ctx.sstable_logic_seq_ = source_meta_handle.get_sstable_meta().get_sstable_seq();
    }
    const ObTabletMeta &dst_meta = ctx_->dst_tablet_handle_.get_obj()->get_tablet_meta();
    if (OB_FAIL(active_ctx.data_desc_.init(true/*DDL output*/, *rewrite_ctx.storage_schema_,
                   ctx_->param_.dest_ls_id_, ctx_->param_.dest_tablet_id_, MINOR_MERGE,
                   active_ctx.end_scn_.get_val_for_tx(),
                   ctx_->param_.data_format_version_, dst_meta.micro_index_clustered_, transfer_epoch,
                   rewrite_plan->parallel_cnt_, ctx_->dst_reorg_scn_, active_ctx.end_scn_,
                   nullptr/*no column group schema*/, 0/*column group index*/, EXEC_MODE_OUTPUT, true/*commit I/O*/,
                   false/*not Inc-Major*/, ObStorageObjectWriteStrategy::WRITE_THROUGH, minor_op_id))) {
      LOG_WARN("[FORKTABLE] failed to init fork minor data desc", K(ret), K(entry_idx_));
    } else if (FALSE_IT(active_ctx.data_desc_.get_static_desc().set_sstable_logic_seq(active_ctx.sstable_logic_seq_))) {
    } else if (OB_FAIL(active_ctx.pre_warm_param_.init(ctx_->param_.dest_ls_id_, ctx_->param_.dest_tablet_id_))) {
      LOG_WARN("[FORKTABLE] failed to init fork minor prewarmer", K(ret), K(entry_idx_));
    } else if (OB_ISNULL(active_ctx.index_builder_ =
                   OB_NEWx(ObSSTableIndexBuilder, &active_ctx.allocator_, false/*disable double write buffer*/))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[FORKTABLE] failed to allocate fork minor index builder", K(ret), K(entry_idx_));
    } else if (OB_FAIL(active_ctx.index_builder_->init(
                   active_ctx.data_desc_.get_desc(), ObSSTableIndexBuilder::DISABLE))) {
      LOG_WARN("[FORKTABLE] failed to init fork minor index builder", K(ret), K(entry_idx_));
    } else {
      active_ctx.data_desc_.get_desc().sstable_index_builder_ = active_ctx.index_builder_;
    }
  }

  ObSEArray<ObITask *, 2> new_tasks;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dag_->alloc_task(finish_task))) {
    LOG_WARN("[FORKTABLE] failed to allocate fork minor finish task", K(ret), K(entry_idx_));
  } else if (OB_FAIL(dag_->alloc_task(rewrite_task))) {
    LOG_WARN("[FORKTABLE] failed to allocate fork minor rewrite task", K(ret), K(entry_idx_));
  } else if (OB_FAIL(finish_task->init(ctx_, entry_idx_))) {
    LOG_WARN("[FORKTABLE] failed to init fork minor finish task", K(ret), K(entry_idx_));
  } else if (OB_FAIL(rewrite_task->init(ctx_, entry_idx_, 0))) {
    LOG_WARN("[FORKTABLE] failed to init fork minor rewrite task", K(ret), K(entry_idx_));
  } else if (OB_FAIL(finish_task->add_child(*wait_task_, false/*skip the static Wait state check*/))) {
    LOG_WARN("[FORKTABLE] failed to attach fork minor finish to wait", K(ret), K(entry_idx_));
  } else if (OB_FAIL(rewrite_task->add_child(*finish_task))) {
    LOG_WARN("[FORKTABLE] failed to attach fork minor rewrite to finish", K(ret), K(entry_idx_));
  } else if (OB_FAIL(add_child(*rewrite_task))) {
    LOG_WARN("[FORKTABLE] failed to attach fork minor prepare to rewrite", K(ret), K(entry_idx_));
  } else if (OB_FAIL(new_tasks.push_back(finish_task))) {
    LOG_WARN("[FORKTABLE] failed to collect fork minor finish task", K(ret), K(entry_idx_));
  } else if (OB_FAIL(new_tasks.push_back(rewrite_task))) {
    LOG_WARN("[FORKTABLE] failed to collect fork minor rewrite task", K(ret), K(entry_idx_));
  } else if (OB_FAIL(dag_->batch_add_task(new_tasks))) {
    LOG_WARN("[FORKTABLE] failed to activate fork minor tasks", K(ret), K(entry_idx_));
  }
  return ret;
}

int ObForkMinorRewriteTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  ObForkMinorRewriteTask *rewrite_task = nullptr;
  ObForkMinorEntry *entry = nullptr;
  ObForkMinorRewritePlan *rewrite_plan = nullptr;
  const int64_t next_task_idx = task_idx_ + 1;
  if (!is_inited_ || OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork minor rewrite task not initialized", K(ret));
  } else if (OB_FAIL(get_fork_minor_rewrite_plan_(ctx_, entry_idx_, entry, rewrite_plan))) {
    LOG_WARN("[FORKTABLE] failed to get next fork minor rewrite plan", K(ret), K(entry_idx_), K(next_task_idx));
  } else if (next_task_idx >= rewrite_plan->parallel_cnt_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(dag_->alloc_task(rewrite_task))) {
    LOG_WARN("[FORKTABLE] failed to allocate next fork minor rewrite task", K(ret), K(entry_idx_), K(next_task_idx));
  } else if (OB_FAIL(rewrite_task->init(ctx_, entry_idx_, next_task_idx))) {
    LOG_WARN("[FORKTABLE] failed to init next fork minor rewrite task", K(ret), K(entry_idx_), K(next_task_idx));
  } else {
    next_task = rewrite_task;
  }
  return ret;
}

int ObForkMinorRewriteTask::process()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ForkRange", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObDataStoreDesc data_desc;
  ObISSTableObjectCleaner *cleaner = nullptr;
  ObDatumRange merge_range;
  ObMacroSeqParam macro_seq_param;
  ObMacroBlockWriter macro_writer;
  ObStoreRowIterator *row_iter = nullptr;
  ObForkMinorEntry *entry = nullptr;
  ObForkMinorRewritePlan *rewrite_plan = nullptr;
  int64_t data_seq = 0;
  int64_t global_slot_idx = -1;
  int64_t row_count = 0;
  if (!is_inited_ || OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork minor rewrite task not initialized", K(ret));
  } else if (OB_FAIL(get_fork_minor_rewrite_plan_(ctx_, entry_idx_, entry, rewrite_plan))) {
    LOG_WARN("[FORKTABLE] failed to get fork minor rewrite plan", K(ret), K(entry_idx_), K(task_idx_));
  } else if (!ctx_->minor_rewrite_.active_.matches(entry_idx_)
      || OB_ISNULL(ctx_->minor_rewrite_.rowkey_read_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] invalid active fork minor rewrite state", K(ret), K(entry_idx_), K(task_idx_));
  } else if (OB_FAIL(rewrite_plan->planner_.get_merge_range(task_idx_, merge_range))) {
    LOG_WARN("[FORKTABLE] failed to get fork minor merge range", K(ret), K(entry_idx_), K(task_idx_));
  } else if (OB_FAIL(data_desc.shallow_copy(ctx_->minor_rewrite_.active_.data_desc_.get_desc()))) {
    LOG_WARN("[FORKTABLE] failed to copy fork minor data desc", K(ret), K(entry_idx_), K(task_idx_));
  } else if (FALSE_IT(global_slot_idx = rewrite_plan->macro_seq_slot_base_ + task_idx_)) {
  } else if (OB_FAIL(ctx_->fork_minor_helper_.generate_minor_macro_seq_info(global_slot_idx, data_seq))) {
    LOG_WARN("[FORKTABLE] failed to generate fork minor data sequence", K(ret), K(entry_idx_), K(task_idx_));
  } else if (OB_FAIL(ObISSTableObjectCleaner::get_cleaner_from_data_store_desc(data_desc, cleaner))) {
    LOG_WARN("[FORKTABLE] failed to get fork minor object cleaner", K(ret), K(entry_idx_), K(task_idx_));
  } else {
    macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    macro_seq_param.start_ = data_seq;
    if (OB_FAIL(macro_writer.open(
            data_desc, task_idx_, macro_seq_param, ctx_->minor_rewrite_.active_.pre_warm_param_, *cleaner))) {
      LOG_WARN("[FORKTABLE] failed to open fork minor macro writer", K(ret), K(entry_idx_), K(task_idx_));
    }
  }

  ObTableAccessParam access_param;
  ObStoreCtx store_ctx;
  ObTableAccessContext access_ctx;
  ObQueryFlag query_flag(ObQueryFlag::Forward/*scan order*/, false/*normal scan*/,
      true/*optimize multi-macro reads*/, true/*whole macro scan*/, false/*partial columns*/,
      false/*no index back*/, false/*disable IO statistics*/);
  ObVersionRange trans_version_range;
  query_flag.disable_cache();
  trans_version_range.snapshot_version_ = ctx_->param_.snapshot_version_;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(access_param.init_merge_param(ctx_->param_.orig_table_id_, ctx_->param_.orig_tablet_id_,
                 *ctx_->minor_rewrite_.rowkey_read_info_, true/*multi-version merge*/))) {
    LOG_WARN("[FORKTABLE] failed to init fork minor access param", K(ret), K(entry_idx_), K(task_idx_));
  } else if (OB_FAIL(store_ctx.init_for_read(ctx_->param_.orig_ls_id_, ctx_->param_.orig_tablet_id_, INT64_MAX,
                 -1/*no lock wait timeout*/, ctx_->fork_snapshot_scn_))) {
    LOG_WARN("[FORKTABLE] failed to init fork minor store context", K(ret), K(entry_idx_), K(task_idx_));
  } else if (OB_FAIL(access_ctx.init(query_flag, store_ctx, allocator, allocator, trans_version_range))) {
    LOG_WARN("[FORKTABLE] failed to init fork minor access context", K(ret), K(entry_idx_), K(task_idx_));
  } else if (FALSE_IT(access_ctx.fork_read_snapshot_version_ = ctx_->param_.snapshot_version_)) {
  } else {
    LOG_INFO("[FORKTABLE] fork minor rewrite reached scan barrier", K(entry_idx_), K(task_idx_),
        "parallel_cnt", rewrite_plan->parallel_cnt_, "macro_seq_slot_base", rewrite_plan->macro_seq_slot_base_,
        K(global_slot_idx), "tablet_id", ctx_->param_.dest_tablet_id_);
    DEBUG_SYNC(BEFORE_FORK_MINOR_REWRITE_SCAN);
#ifdef ERRSIM
    ret = EN_FORK_MINOR_REWRITE_TASK_FAIL ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("[FORKTABLE] injected fork minor rewrite task failure", K(ret), K(entry_idx_), K(task_idx_),
          "tablet_id", ctx_->param_.dest_tablet_id_);
    }
#endif
    if (OB_SUCC(ret) && OB_FAIL(ctx_->minor_rewrite_.active_.source_->scan(
            access_param.iter_param_, access_ctx, merge_range, row_iter))) {
      LOG_WARN("[FORKTABLE] failed to scan fork minor range", K(ret), K(entry_idx_), K(task_idx_), K(merge_range));
    } else if (OB_SUCC(ret) && OB_ISNULL(row_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] fork minor row iterator is null", K(ret), K(entry_idx_), K(task_idx_));
    }
  }
  const ObDatumRow *row = nullptr;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(row_iter->get_next_row(row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("[FORKTABLE] failed to get fork minor row", K(ret), K(entry_idx_), K(task_idx_));
      }
      break;
    } else if (OB_ISNULL(row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] fork minor row is null", K(ret), K(entry_idx_), K(task_idx_));
    } else if (ctx_->minor_rewrite_.active_.source_->is_minor_sstable()
        && OB_FAIL(ObForkTableDataDag::normalize_fork_minor_snapshot_row_(
                       ctx_->minor_rewrite_.storage_schema_->get_rowkey_column_num(),
                       *const_cast<ObDatumRow *>(row)))) {
      LOG_WARN("[FORKTABLE] failed to normalize fork minor row", K(ret), K(entry_idx_), K(task_idx_));
    } else if (OB_FAIL(macro_writer.append_row(*row, nullptr))) {
      LOG_WARN("[FORKTABLE] failed to append fork minor row", K(ret), K(entry_idx_), K(task_idx_));
    } else {
      ++row_count;
    }
  }
  if (OB_NOT_NULL(row_iter)) {
    row_iter->~ObStoreRowIterator();
    row_iter = nullptr;
  }
  if (OB_SUCC(ret) && OB_FAIL(macro_writer.close())) {
    LOG_WARN("[FORKTABLE] failed to close fork minor macro writer", K(ret), K(entry_idx_), K(task_idx_));
  } else if (OB_SUCC(ret)) {
    LOG_INFO("[FORKTABLE] rewrote fork minor range", K(entry_idx_), K(task_idx_),
        "parallel_cnt", rewrite_plan->parallel_cnt_, "macro_seq_slot_base", rewrite_plan->macro_seq_slot_base_,
        K(global_slot_idx), "tablet_id", ctx_->param_.dest_tablet_id_, K(row_count), K(merge_range));
  }
  return ret;
}

int ObForkMinorFinishTask::process()
{
  int ret = OB_SUCCESS;
  int64_t index_seq = 0;
  int64_t index_global_slot_idx = -1;
  ObForkMinorEntry *entry = nullptr;
  ObForkMinorRewritePlan *rewrite_plan = nullptr;
  ObSSTableMergeRes merge_res;
  ObITable::TableKey table_key;
  ObTabletCreateSSTableParam create_param;
  if (!is_inited_ || OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork minor finish task not initialized", K(ret));
  } else if (OB_FAIL(get_fork_minor_rewrite_plan_(ctx_, entry_idx_, entry, rewrite_plan))) {
    LOG_WARN("[FORKTABLE] failed to get fork minor finish plan", K(ret), K(entry_idx_));
  } else if (!ctx_->minor_rewrite_.active_.matches(entry_idx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] invalid active fork minor finish state", K(ret), K(entry_idx_));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_FORK_MINOR_FINISH_TASK_FAIL ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_WARN("[FORKTABLE] injected fork minor finish task failure", K(ret), K(entry_idx_),
          "tablet_id", ctx_->param_.dest_tablet_id_);
    }
  }
#endif
  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(index_global_slot_idx = rewrite_plan->macro_seq_slot_base_ + rewrite_plan->parallel_cnt_)) {
  } else if (OB_FAIL(ctx_->fork_minor_helper_.generate_minor_macro_seq_info(index_global_slot_idx, index_seq))) {
    LOG_WARN("[FORKTABLE] failed to generate fork minor index sequence", K(ret), K(entry_idx_));
  } else if (OB_FAIL(ctx_->minor_rewrite_.active_.index_builder_->close_with_macro_seq(merge_res, index_seq,
                 OB_DEFAULT_MACRO_BLOCK_SIZE, 0/*nested offset*/, ctx_->minor_rewrite_.active_.pre_warm_param_))) {
    LOG_WARN("[FORKTABLE] failed to close fork minor index builder", K(ret), K(entry_idx_), K(index_seq));
  } else {
    table_key.tablet_id_ = ctx_->param_.dest_tablet_id_;
    table_key.column_group_idx_ = 0;
    table_key.table_type_ = ObITable::TableType::MINOR_SSTABLE;
    table_key.scn_range_.start_scn_ = ctx_->minor_rewrite_.active_.source_->get_start_scn();
    table_key.scn_range_.end_scn_ = ctx_->minor_rewrite_.active_.end_scn_;
    if (OB_FAIL(create_param.init_for_fork_minor(table_key, *ctx_->minor_rewrite_.storage_schema_,
                   ctx_->minor_rewrite_.active_.sstable_logic_seq_,
                   ctx_->minor_rewrite_.active_.source_->get_rec_scn(), merge_res))) {
      LOG_WARN("[FORKTABLE] failed to init fork minor create param", K(ret), K(entry_idx_), K(table_key));
    } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(
                   create_param, ctx_->output_allocator_, ctx_->minor_entries_.at(entry_idx_).output_handle_))) {
      LOG_WARN("[FORKTABLE] failed to create fork rewritten minor", K(ret), K(entry_idx_), K(create_param));
    } else {
      LOG_INFO("[FORKTABLE] created one fork minor output", K(entry_idx_), K(index_global_slot_idx), K(table_key));
      ctx_->minor_rewrite_.active_.reset();
    }
  }
  return ret;
}

int ObForkTablePublishTask::process()
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  ObLS *dst_ls = nullptr;
  const ObSSTable *last_minor = nullptr;
  ObSSMetaCtrlKeyInfo minor_ctrl_key_info;
  share::SCN max_end_scn = share::SCN::min_scn();
  const bool has_started_minor_op = OB_NOT_NULL(ctx_) && ctx_->fork_minor_helper_.has_started_op();
  if (!is_inited_ || OB_ISNULL(ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork publish task not initialized", K(ret));
  } else if (OB_ISNULL(meta_service) || OB_ISNULL(dst_ls = ctx_->dst_ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] fork publish dependency is null", K(ret), KP(meta_service), KP(dst_ls));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->minor_entries_.count(); ++i) {
    const ObSSTable *minor = nullptr;
    if (OB_FAIL(ctx_->minor_entries_.at(i).output_handle_.get_sstable(minor))) {
      LOG_WARN("[FORKTABLE] failed to get fork minor output", K(ret), K(i));
    } else if (OB_ISNULL(minor)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] fork minor output is null", K(ret), K(i));
    } else if (OB_FAIL(ctx_->tables_handle_.add_table(ctx_->minor_entries_.at(i).output_handle_))) {
      LOG_WARN("[FORKTABLE] failed to add fork minor output", K(ret), K(i));
    } else if (minor->get_end_scn() > max_end_scn) {
      max_end_scn = minor->get_end_scn();
      last_minor = minor;
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(last_minor)) {
    bool need_fill = false;
    share::SCN fill_end_scn;
    ObSSTableMetaHandle meta_handle;
    if (OB_FAIL(ObTabletSplitUtil::check_need_fill_empty_sstable(ctx_->src_ls_handle_, true/*check Minor SSTable*/,
                   last_minor->get_key(), ctx_->param_.dest_tablet_id_, need_fill, fill_end_scn))) {
      LOG_WARN("[FORKTABLE] failed to check fork empty minor", K(ret), K(ctx_->param_));
    } else if (need_fill) {
      ObTableHandleV2 empty_handle;
      ObTabletCreateSSTableParam create_param;
      if (OB_FAIL(last_minor->get_meta(meta_handle))) {
        LOG_WARN("[FORKTABLE] failed to get last fork minor meta", K(ret));
      } else if (OB_FAIL(create_param.init_for_split_empty_minor_sstable(ctx_->param_.dest_tablet_id_,
                     last_minor->get_key().get_end_scn(), fill_end_scn,
                     meta_handle.get_sstable_meta().get_basic_meta()))) {
        LOG_WARN("[FORKTABLE] failed to init fork empty minor param", K(ret), K(fill_end_scn));
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(
                     create_param, ctx_->output_allocator_, empty_handle))) {
        LOG_WARN("[FORKTABLE] failed to create fork empty minor", K(ret), K(create_param));
      } else if (OB_FAIL(ctx_->tables_handle_.add_table(empty_handle))) {
        LOG_WARN("[FORKTABLE] failed to add fork empty minor", K(ret));
      } else {
        LOG_INFO("[FORKTABLE] fork filled empty minor", K(ctx_->param_), K(fill_end_scn));
      }
    }
  }

  ObBatchUpdateTableStoreParam batch_param;
  ObArray<ObSSMetaCtrlKeyInfo> meta_ctrl_keys;
  ObArenaAllocator tmp_arena("ForkPubSS", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTabletHandle ss_tablet_handle;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(meta_service->get_tablet(dst_ls->get_ls_id(), ctx_->param_.dest_tablet_id_,
                 ctx_->dst_reorg_scn_, tmp_arena, ss_tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_FAIL(meta_service->create_tablet(
              dst_ls->get_ls_id(), ctx_->param_.dest_tablet_id_, ctx_->dst_reorg_scn_, true/*check lease*/))) {
        LOG_WARN("[FORKTABLE] failed to create shared fork tablet", K(ret), K(ctx_->param_));
      }
    } else {
      LOG_WARN("[FORKTABLE] failed to get shared fork tablet", K(ret), K(ctx_->param_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObForkTabletUtil::build_fork_replace_table_store_param(ctx_->dst_reorg_scn_,
                 dst_ls->get_rebuild_seq(), ctx_->param_.snapshot_version_, ctx_->param_.snapshot_version_,
                 ctx_->param_.dest_tablet_id_, ctx_->tables_handle_, ctx_->fork_snapshot_scn_, batch_param))) {
    LOG_WARN("[FORKTABLE] failed to build fork replace table store param", K(ret), K(ctx_->param_));
  } else if (has_started_minor_op && OB_FAIL(ctx_->fork_minor_helper_.get_ctrl_key_info(minor_ctrl_key_info))) {
    LOG_WARN("[FORKTABLE] failed to get fork minor ctrl key", K(ret), K(ctx_->param_));
  } else if (has_started_minor_op && OB_FAIL(meta_ctrl_keys.push_back(minor_ctrl_key_info))) {
    LOG_WARN("[FORKTABLE] failed to collect fork minor ctrl key", K(ret), K(ctx_->param_));
  } else if (has_started_minor_op && OB_FAIL(ctx_->fork_minor_helper_.finish_add_op(true/*finish operation*/))) {
    LOG_WARN("[FORKTABLE] failed to finish fork minor add op", K(ret), K(ctx_->param_));
  } else {
    DEBUG_SYNC(BEFORE_FORK_PUBLISH_REPLACE);
    if (OB_FAIL(meta_service->build_tablet_with_batch_tables(dst_ls->get_ls_id(),
            ctx_->param_.dest_tablet_id_, ctx_->dst_reorg_scn_,
            ObMetaUpdateReason::TABLET_FORK_REPLACE_SSTABLES, meta_ctrl_keys, batch_param))) {
      LOG_WARN("[FORKTABLE] failed to replace fork tables in shared meta", K(ret), K(ctx_->param_));
    }
  }
  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObForkTableReservedScnOperator reserved_scn_op;
    if (OB_ISNULL(GCTX.sql_proxy_)) {
      LOG_WARN("sql proxy is null, skip done stamp (scheduler will transcribe)");
    } else if (OB_SUCCESS != (tmp_ret = reserved_scn_op.init(*GCTX.sql_proxy_, 10 * 1000 * 1000L))) {
      LOG_WARN("failed to init reserved scn operator for done stamp", KR(tmp_ret), K(ctx_->param_));
    } else if (OB_SUCCESS != (tmp_ret = reserved_scn_op.update_tablet_done(ctx_->param_.dest_tenant_id_,
                   ctx_->param_.dest_ls_id_, ctx_->param_.task_id_, ctx_->param_.dest_tablet_id_))) {
      LOG_WARN("[FORKTABLE] failed to stamp fork table reserved scn done (best-effort)", KR(tmp_ret), K(ctx_->param_));
    }
    LOG_INFO("[FORKTABLE] fork data atomically published to SS (await per-replica local land)", K(ctx_->param_),
        "minor_cnt", ctx_->minor_entries_.count());
  }
  return ret;
}

// local-land 必须重新读取 publish 后的 shared tablet，不能复用 publish 前的本地 table handle。
int ObForkTableDataDag::collect_fork_local_land_tables_(const share::SCN &fork_snapshot_scn,
    ObTableStoreIterator &table_store_iter, ObTablesHandleArray &local_sstables_handle)
{
  int ret = OB_SUCCESS;
  local_sstables_handle.reset();
  while (OB_SUCC(ret)) {
    ObITable *table = nullptr;
    ObTableHandleV2 table_handle;
    if (OB_FAIL(table_store_iter.get_next(table_handle))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("[FORKTABLE] failed to get next fork land table", K(ret));
      }
    } else if (OB_ISNULL(table = table_handle.get_table()) || OB_UNLIKELY(!table->is_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] unexpected fork land table", K(ret), KPC(table));
    } else if (OB_UNLIKELY(table->is_meta_major_sstable())) {
      // d803cdd0 已禁用全部 Meta Major 调度入口，local-land 不应出现 Meta Major。
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[FORKTABLE] unexpected meta major sstable after scheduling disabled", K(ret), KPC(table));
    } else if (table->is_minor_sstable() && table->get_end_scn() > fork_snapshot_scn) {
      // SSWriter 可能在 publish 后上传 fork snapshot 之后的 MINI/MINOR，local-land 必须过滤这部分数据。
      continue;
    } else if (OB_FAIL(local_sstables_handle.add_table(table_handle))) {
      LOG_WARN("[FORKTABLE] failed to add fork local land sstable", K(ret), KPC(table));
    }
  }
  return ret;
}

int ObForkTableDataDag::land_fork_tables_to_local_(const ObForkTableDataParam &param, ObLSHandle &ls_handle,
    const share::SCN &reorg_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLS *ls = ls_handle.get_ls();
  ObArenaAllocator tmp_arena("ForkLandLoc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTabletHandle ss_tablet_handle;
  ObTableStoreIterator ss_table_store_iter;
  ObTablesHandleArray local_sstables_handle;
  ObTablesHandleArray marked_local_sstables_handle;
  ObBatchUpdateTableStoreParam local_param;
  share::SCN fork_snapshot_scn;
  share::SCN source_row_meta_scn;
  if (OB_ISNULL(ls)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(param));
  } else if (OB_FAIL(fork_snapshot_scn.convert_for_tx(param.snapshot_version_))) {
    LOG_WARN("[FORKTABLE] failed to convert fork snapshot scn", K(ret), K(param));
  } else if (OB_FAIL(MTL(ObSSMetaService *)->get_tablet(
      ls->get_ls_id(), param.dest_tablet_id_, reorg_scn, tmp_arena,
      ss_tablet_handle, source_row_meta_scn))) {
    LOG_WARN("[FORKTABLE] failed to re-read shared fork tablet for local landing", K(ret), K(param), K(reorg_scn));
  } else if (OB_UNLIKELY(!ss_tablet_handle.is_valid() || OB_ISNULL(ss_tablet_handle.get_obj()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] shared fork tablet is invalid after publish", K(ret), K(param));
  } else if (OB_FAIL(ss_tablet_handle.get_obj()->get_all_sstables(ss_table_store_iter, false/*不展开*/))) {
    LOG_WARN("[FORKTABLE] failed to get all sstables from shared fork tablet", K(ret), K(param));
  } else if (OB_FAIL(collect_fork_local_land_tables_(fork_snapshot_scn, ss_table_store_iter, local_sstables_handle))) {
    LOG_WARN("[FORKTABLE] failed to collect fork local land tables", K(ret), K(param));
  } else if (OB_FAIL(ObTableStoreUtil::set_shared_meta_scn_for_inc_major_tables(
      local_sstables_handle, source_row_meta_scn, tmp_arena, marked_local_sstables_handle))) {
    LOG_WARN("[FORKTABLE] failed to set shared meta scn for inc major tables", K(ret), K(param));
  } else if (OB_FAIL(ObForkTabletUtil::build_fork_replace_table_store_param(
      reorg_scn, ls->get_rebuild_seq(), param.snapshot_version_, param.snapshot_version_,
      param.dest_tablet_id_, marked_local_sstables_handle, fork_snapshot_scn, local_param))) {
    LOG_WARN("[FORKTABLE] failed to build fork local land table store param", K(ret), K(param));
  } else {
    for (int64_t i = 0; i < local_sstables_handle.get_count(); ++i) {
      const ObITable *table = local_sstables_handle.get_table(i);
      if (table->is_minor_sstable()) {
        local_param.source_minor_tablet_meta_scn_ = source_row_meta_scn;
      }
    }
    LOG_INFO("[FORKTABLE] reached before fork local land replace",
        "dest_tablet_id", param.dest_tablet_id_.id(), K(reorg_scn), K(fork_snapshot_scn),
        "table_count", local_sstables_handle.get_count());
    DEBUG_SYNC(BEFORE_FORK_LOCAL_LAND_REPLACE);
#ifdef ERRSIM
    if (OB_UNLIKELY(EN_FORK_TABLE_BLOCK_LOCAL_LAND)) {
      ret = OB_EAGAIN;
      LOG_INFO("[FORKTABLE] EN_FORK_TABLE_BLOCK_LOCAL_LAND: skip fork local land replace this round",
          KR(ret), K(param), K(reorg_scn));
      SERVER_EVENT_ADD("FORK_TABLE_TEST", "LOCAL_LAND_BLOCKED",
          "tenant_id", param.dest_tenant_id_, "ls_id", param.dest_ls_id_.id(),
          "tablet_id", param.dest_tablet_id_.id(), "fork_snapshot", param.snapshot_version_,
          "src_tablet_id", param.orig_tablet_id_.id());
    }
#endif
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ls->build_tablet_with_batch_tables(param.dest_tablet_id_, local_param))) {
      LOG_WARN("[FORKTABLE] failed to replace fork tables to local tablet", K(ret), K(param));
    } else {
      ObStorageCachePolicyService *storage_cache_policy_service = MTL(ObStorageCachePolicyService *);
      if (OB_ISNULL(storage_cache_policy_service)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FORKTABLE] fail to get storage cache policy service after local land",
            KR(tmp_ret), K(param), K(reorg_scn));
      // TODO @fangdan: master上改成同步接口
      } else if (OB_TMP_FAIL(storage_cache_policy_service->async_prewarm_tablet(
          param.dest_tablet_id_.id(), PolicyStatus::HOT))) {
        LOG_WARN("[FORKTABLE] fail to trigger hot tablet prewarm after local land",
            KR(tmp_ret), K(param), K(reorg_scn));
      }
      LOG_INFO("[FORKTABLE] replaced fork tables to local tablet", K(param), K(reorg_scn),
          "table_count", local_sstables_handle.get_count());
#ifdef ERRSIM
      SERVER_EVENT_ADD("FORK_TABLE_TEST", "LOCAL_LANDED",
          "tenant_id", param.dest_tenant_id_, "ls_id", param.dest_ls_id_.id(),
          "tablet_id", param.dest_tablet_id_.id(), "fork_snapshot", param.snapshot_version_,
          "src_tablet_id", param.orig_tablet_id_.id());
#endif
    }
  }
  return ret;
}

static int verify_fork_gc_reserved_before_build_(const ObForkTableDataParam &param)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = param.dest_tenant_id_;
  const int64_t timeout_us = 10 * 1000 * 1000L;
  ObForkTableReservedScnRow row;
  bool is_exist = false;
  // GC 保护按组生效，检查任意组行即可，不能依赖当前 tablet 的观测行。
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork param for reserve shared meta", K(ret), K(param));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else {
    ObForkTableReservedScnOperator op;
    if (OB_FAIL(op.init(*GCTX.sql_proxy_, timeout_us))) {
      LOG_WARN("[FORKTABLE] failed to init fork table reserved scn operator", K(ret));
    } else if (OB_FAIL(op.get_group_any_row(tenant_id, param.dest_ls_id_, param.task_id_,
                                            row, is_exist))) {
      LOG_WARN("[FORKTABLE] failed to get fork table reserved scn group row", K(ret), K(param));
    } else if (!is_exist) {
      ret = OB_EAGAIN;
      LOG_WARN("[FORKTABLE] fork table reserved scn group not reserved, wait scheduler reserve and retry", K(ret),
          "tenant_id", tenant_id, "ls_id", param.dest_ls_id_, "task_id", param.task_id_,
          "dst_tablet_id", param.dest_tablet_id_);
    } else {
      LOG_INFO("[FORKTABLE] fork gc reserved verified before build", K(row));
    }
  }
  return ret;
}

int ObForkTableDataDag::check_ss_fork_data_complete(
    const share::ObLSID &dest_ls_id,
    const ObTabletID &dst_tablet_id,
    const share::SCN &fork_snapshot_scn,
    const ObTabletID &src_tablet_id,
    bool &done)
{
  int ret = OB_SUCCESS;
  done = false;
  ObArenaAllocator arena("ForkChkSS", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTabletHandle ss_tablet_handle;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  if (OB_UNLIKELY(!dest_ls_id.is_valid() || !dst_tablet_id.is_valid()
      || !fork_snapshot_scn.is_valid_and_not_min() || !src_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid args", K(ret), K(dest_ls_id), K(dst_tablet_id), K(fork_snapshot_scn), K(src_tablet_id));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(dest_ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get dest ls", K(ret), K(dest_ls_id));
  } else if (OB_FAIL(ObTabletSplitUtil::get_tablet(arena, ls_handle, dst_tablet_id,
                 true/*is_shared_mode*/, ss_tablet_handle, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_ENTRY_NOT_EXIST == ret || OB_EMPTY_RESULT == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("[FORKTABLE] failed to get ss fork tablet", K(ret), K(dest_ls_id), K(dst_tablet_id));
    }
  } else {
    const share::ObForkTabletInfo &ss_fork = ss_tablet_handle.get_obj()->get_tablet_meta().fork_info_;
    if (ss_fork.is_valid()
        && ss_fork.get_fork_snapshot_scn() == fork_snapshot_scn
        && ss_fork.get_src_tablet_id() == src_tablet_id) {
      done = ss_fork.is_data_complete();
    } else {
      LOG_INFO("[FORKTABLE] ss fork info not match, treat as not published (stale)",
          K(dst_tablet_id), K(fork_snapshot_scn), K(ss_fork));
    }
  }
  return ret;
}

int ObForkTableDataDag::submit_fork_ttl_mds_reloc_if_needed_(const share::SCN &fork_snapshot_scn)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle src_ls_handle;
  ObLSHandle dst_ls_handle;
  ObTabletHandle src_tablet_handle;
  ObTabletHandle dst_tablet_handle;
  ObTablet *src_tablet = nullptr;
  ObTablet *dst_tablet = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;
  share::ObForkTabletInfo fork_info;
  share::SCN dst_reorg_scn;
  bool is_primary_tenant = false;
  bool is_mds_reloc_done = false;
  if (OB_UNLIKELY(!fork_snapshot_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork snapshot for mds reloc", K(ret), K(fork_snapshot_scn), K(param_));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(param_.dest_ls_id_, dst_ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get dest ls", K(ret), K(param_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(dst_ls_handle, param_.dest_tablet_id_, dst_tablet_handle,
                                               ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get dest tablet", K(ret), K(param_));
  } else if (OB_ISNULL(dst_tablet = dst_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest tablet is null", K(ret), K(param_));
  } else if (OB_FAIL(dst_tablet->get_fork_info(fork_info))) {
    LOG_WARN("[FORKTABLE] failed to get dest fork info for mds reloc", K(ret), K(param_));
  } else if (FALSE_IT(dst_reorg_scn = dst_tablet->get_reorganization_scn())) {
  } else if (OB_UNLIKELY(!dst_reorg_scn.is_valid()
      || fork_info.get_fork_snapshot_scn() != fork_snapshot_scn
      || fork_info.get_src_tablet_id() != param_.orig_tablet_id_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[FORKTABLE] dest fork generation mismatches mds reloc task", K(ret),
        K(param_), K(fork_snapshot_scn), K(dst_reorg_scn), K(fork_info));
  } else if (OB_FAIL(dst_tablet->get_latest_committed_tablet_status(user_data))) {
    LOG_WARN("[FORKTABLE] failed to get committed dest status for mds reloc", K(ret), K(param_), K(fork_snapshot_scn));
  } else if (ObTabletStatus::FORK_DST != user_data.tablet_status_) {
    ret = OB_EAGAIN;
    LOG_WARN("[FORKTABLE] dest tablet is not in FORK_DST for mds reloc",
        K(ret), K(param_), K(fork_snapshot_scn), K(user_data));
  } else if (OB_FAIL(check_fork_mds_reloc_done_(dst_ls_handle, param_.dest_tablet_id_, dst_reorg_scn,
                                                is_mds_reloc_done))) {
    LOG_WARN("[FORKTABLE] failed to check fork ttl mds reloc marker", K(ret), K(param_),
        K(fork_snapshot_scn), K(dst_reorg_scn));
  } else if (is_mds_reloc_done) {
    LOG_INFO("[FORKTABLE] fork ttl mds relocation already completed",
        K(param_), K(fork_snapshot_scn), K(dst_reorg_scn));
  } else if (OB_FAIL(ObStorageHAUtils::check_is_primary_tenant(
                 param_.dest_tenant_id_, is_primary_tenant))) {
    LOG_WARN("[FORKTABLE] failed to check primary tenant before mds reloc", K(ret), K(param_));
  } else if (!is_primary_tenant) {
    ret = OB_EAGAIN;
    LOG_INFO("[FORKTABLE] wait fork ttl mds reloc final marker on standby",
        K(ret), K(param_), K(fork_snapshot_scn), K(dst_reorg_scn));
  } else if (OB_FAIL(ls_service->get_ls(param_.orig_ls_id_, src_ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get source ls", K(ret), K(param_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(
                 src_ls_handle, param_.orig_tablet_id_, src_tablet_handle,
                 ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get source tablet", K(ret), K(param_));
  } else if (OB_ISNULL(src_tablet = src_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("source tablet is null", K(ret), K(param_));
  } else if (OB_FAIL(ObLsMdsTabletForkReloc::submit_fork_ttl_mds_reloc(
                 *src_tablet, param_.dest_tablet_id_, fork_snapshot_scn,
                 dst_reorg_scn, param_.dest_tenant_id_, param_.dest_ls_id_))) {
    LOG_WARN("[FORKTABLE] failed to relocate fork ttl mds", K(ret), K(param_), K(fork_snapshot_scn), K(dst_reorg_scn));
  } else {
    LOG_INFO("[FORKTABLE] fork ttl mds relocation submitted before data publish",
        K(param_), K(fork_snapshot_scn), K(dst_reorg_scn));
  }
  return ret;
}

int ObForkTableDataDag::prepare_fork_publish(ObITask &prepare_task)
{
  int ret = OB_SUCCESS;
  bool done = false;
  share::SCN fork_snapshot;
  if (OB_UNLIKELY(!is_inited_ || !param_.is_valid() || ObForkDataPhase::FORK_PUBLISH != fork_phase_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork publish dag not init or wrong phase", K(ret), K_(is_inited), K_(param),
        "fork_phase", static_cast<int64_t>(fork_phase_));
  } else if (OB_FAIL(fork_snapshot.convert_for_tx(param_.snapshot_version_))) {
    LOG_WARN("[FORKTABLE] failed to convert fork snapshot", K(ret), K(param_));
  } else if (OB_FAIL(check_ss_fork_data_complete(param_.dest_ls_id_, param_.dest_tablet_id_,
                 fork_snapshot, param_.orig_tablet_id_, done))) {
    LOG_WARN("[FORKTABLE] failed to check ss fork data complete", K(ret), K(param_));
  } else if (done) {
    LOG_INFO("[FORKTABLE] ss fork data already published, skip rebuild", K(param_));
  } else if (OB_FAIL(verify_fork_gc_reserved_before_build_(param_))) {
    LOG_WARN("[FORKTABLE] failed to reserve shared meta before fork build", K(ret), K(param_));
  } else if (OB_FAIL(submit_fork_ttl_mds_reloc_if_needed_(fork_snapshot))) {
    LOG_WARN("[FORKTABLE] fork inherited ttl mds is not ready before data publish", K(ret), K(param_));
  } else if (OB_FAIL(generate_fork_publish_tasks_(publish_allocator_, prepare_task))) {
    LOG_WARN("[FORKTABLE] failed to generate fork publish tasks", K(ret), K(param_));
  }
  return ret;
}

ObForkTablePrepareTask::ObForkTablePrepareTask()
  : ObITask(TASK_TYPE_COMPLEMENT_PREPARE), is_inited_(false), dag_(nullptr)
{
}

ObForkTablePrepareTask::~ObForkTablePrepareTask()
{
}

int ObForkTablePrepareTask::init(ObForkTableDataDag *dag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[FORKTABLE] ObForkTablePrepareTask has already been inited", K(ret));
  } else if (OB_ISNULL(dag) || !dag->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork table data dag", K(ret), KP(dag));
  } else {
    dag_ = dag;
    is_inited_ = true;
  }
  return ret;
}

int ObForkTablePrepareTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(dag_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork table prepare task not init", K(ret), K_(is_inited), KP_(dag));
  } else if (OB_FAIL(dag_->prepare_fork_publish(*this))) {
    LOG_WARN("[FORKTABLE] failed to prepare fork publish", K(ret), KPC(dag_));
  }
  return ret;
}

int ObForkTableDataDag::process_fork_local_land()
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObTabletHandle dst_tablet_handle;
  ObTablet *dst_tablet = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;
  share::SCN dst_reorg_scn;
  bool is_mds_reloc_done = false;
  if (OB_UNLIKELY(!is_inited_
      || !param_.is_valid()
      || ObForkDataPhase::FORK_LOCAL_LAND != fork_phase_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork local land dag not init or wrong phase",
        K(ret), K_(is_inited), K_(param),
        "fork_phase", static_cast<int64_t>(fork_phase_));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(
                 param_.dest_ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get dest ls", K(ret), K(param_));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(
                 ls_handle, param_.dest_tablet_id_, dst_tablet_handle,
                 ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("[FORKTABLE] failed to get committed fork dst for local land, retry",
        K(ret), K(param_));
  } else if (OB_ISNULL(dst_tablet = dst_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] fork dst tablet is null before local land", K(ret), K(param_));
  } else if (dst_tablet->get_tablet_meta().fork_info_.is_data_complete()) {
    LOG_INFO("[FORKTABLE] local fork data already landed, skip download", K(param_));
  } else if (OB_FAIL(dst_tablet->get_latest_committed_tablet_status(user_data))) {
    LOG_WARN("[FORKTABLE] failed to get fork dst status before local land", K(ret), K(param_));
  } else if (ObTabletStatus::FORK_DST != user_data.tablet_status_) {
    ret = OB_EAGAIN;
    LOG_INFO("[FORKTABLE] wait fork dst status before local land", K(ret), K(param_), K(user_data));
  } else if (FALSE_IT(dst_reorg_scn = dst_tablet->get_reorganization_scn())) {
  } else if (OB_FAIL(check_fork_mds_reloc_done_(ls_handle, param_.dest_tablet_id_, dst_reorg_scn, is_mds_reloc_done))) {
    LOG_WARN("[FORKTABLE] failed to check fork ttl mds reloc marker before local land", K(ret), K(param_),
        K(dst_reorg_scn));
  } else if (!is_mds_reloc_done) {
    ret = OB_EAGAIN;
    LOG_INFO("[FORKTABLE] wait fork ttl mds reloc marker before local land",
        K(ret), K(param_), K(dst_reorg_scn));
  } else if (OB_FAIL(land_fork_tables_to_local_(param_, ls_handle, dst_reorg_scn))) {
    LOG_WARN("[FORKTABLE] failed to replace fork tables to local", K(ret), K(param_), K(dst_reorg_scn));
  }
  return ret;
}

ObForkDownloadSSTableTask::ObForkDownloadSSTableTask()
  : ObITask(TASK_TYPE_COMPLEMENT_MERGE), is_inited_(false), dag_(nullptr)
{
}

ObForkDownloadSSTableTask::~ObForkDownloadSSTableTask()
{
}

int ObForkDownloadSSTableTask::init(ObForkTableDataDag *dag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[FORKTABLE] ObForkDownloadSSTableTask has already been inited", K(ret));
  } else if (OB_ISNULL(dag) || !dag->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork table data dag", K(ret), KP(dag));
  } else {
    dag_ = dag;
    is_inited_ = true;
  }
  return ret;
}

int ObForkDownloadSSTableTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(dag_))) {
    ret = OB_NOT_INIT;
    LOG_WARN("[FORKTABLE] fork download task not init", K(ret), K_(is_inited), KP_(dag));
  } else if (OB_FAIL(dag_->process_fork_local_land())) {
    LOG_WARN("[FORKTABLE] failed to process fork local land", K(ret), KPC(dag_));
  }
  return ret;
}


} // namespace storage
} // namespace oceanbase

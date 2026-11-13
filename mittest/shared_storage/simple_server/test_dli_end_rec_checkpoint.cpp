/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include "deps/oblib/unittest/lib/unittest_diagnostic_info_util.h"
#include "test_shared_meta_common.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#define private public
#define protected public
#include "storage/ddl/ob_ddl_replay_executor.h"
#include "storage/ddl/ob_ddl_redo_log_replayer.h"
#include "storage/ddl/ob_inc_ddl_merge_task_utils.h"
#include "storage/ddl/ob_inc_ddl_merge_helper.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_ddl_merge_schedule.h"
#include "storage/ddl/ob_ddl_inc_task.h"
#include "storage/ddl/ob_ss_ddl_util.h"
#include "storage/compaction_v2/ob_ss_compact_helper.h"
#include "storage/direct_load/ob_direct_load_ss_update_inc_major_task.h"
#include "storage/high_availability/ob_ss_transfer_tablet_backfill_tx.h"
#include "storage/incremental/ob_inc_sstable_upload_task.h"
#include "storage/tablet/ob_tablet_ddl_complete_mds_helper.h"
#include "storage/compaction/ob_tablet_medium_clog_replay_executor.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/tx/ob_tx_log.h"
#include "logservice/ob_log_handler.h"
#include "share/ob_async_index_refresh_util.h"
#include "unittest/storage/init_basic_struct.h"
#include "storage/schema_utils.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#undef private
#undef protected

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT

#ifdef ERRSIM
thread_local int64_t ddl_merge_dag_create_count = 0;

int compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(
    storage::ObDDLTableMergeDagParam &param, const bool is_emergency)
{
  // Count synchronous preparation requests while retaining real DAG creation and enqueue deduplication.
  ++ddl_merge_dag_create_count;
  return MTL(share::ObTenantDagScheduler *)->create_and_add_dag<storage::ObDDLTableMergeDag>(&param, is_emergency);
}
#endif
}

int64_t lease_epoch = 1;
bool mock_sswriter_enabled = true;
thread_local int64_t check_lease_call_count = 0;
thread_local bool mock_is_sswriter = true;
thread_local bool mock_sswriter_addr_valid = true;
static bool block_compaction_writer = false;

int ObSSWriterService::check_lease(const ObSSWriterKey &key, bool &is_writer, int64_t &epoch)
{
  ++check_lease_call_count;
  is_writer = ATOMIC_LOAD(&mock_sswriter_enabled) && mock_is_sswriter
      && !(ATOMIC_LOAD(&block_compaction_writer)
          && key.get_type() == ObSSWriterType::COMPACTION);
  epoch = ATOMIC_LOAD(&lease_epoch);
  return OB_SUCCESS;
}

int ObSSWriterService::check_lease_by_palf_role(
    const ObSSWriterKey &key, bool &is_writer, int64_t &epoch)
{
  return check_lease(key, is_writer, epoch);
}

int ObSSWriterService::get_sswriter_addr(
    const ObSSWriterKey &, ObSSWriterAddr &addr, const int64_t)
{
  if (mock_sswriter_addr_valid) {
    addr.addr_ = GCTX.self_addr();
    addr.epoch_ = ATOMIC_LOAD(&lease_epoch);
  } else {
    addr.reset();
  }
  return OB_SUCCESS;
}

namespace oceanbase
{
char *shared_storage_info = nullptr;
bool new_tenant_created = false;
namespace unittest
{
TestRunCtx TestSharedMetaService::run_ctx_;

void TestSharedMetaService::TearDownTestCase()
{
  ResidualDataCleanerHelper::clean_in_mock_env();
  ObSimpleClusterTestBase::TearDownTestCase();
}

class RecEndRoleGuard
{
public:
  RecEndRoleGuard() : role_(MTL_GET_TENANT_ROLE_CACHE()) {}
  ~RecEndRoleGuard() { restore(); }
  void restore()
  {
    MTL_SET_TENANT_ROLE_CACHE(role_);
    mock_is_sswriter = true;
  }
private:
  share::ObTenantRole::Role role_;
};

struct RecEndContext
{
  RecEndContext()
      : allocator_(ObMemAttr(MTL_ID(), "RecEndTest")), schema_(nullptr) {}
  ~RecEndContext() { ObTabletObjLoadHelper::free(allocator_, schema_); }
  ObArenaAllocator allocator_;
  ObTabletID tablet_id_;
  ObLSID ls_id_;
  ObLSHandle ls_;
  ObTabletHandle local_;
  ObTabletHandle shared_;
  ObStorageSchema *schema_;
  SCN reorganization_scn_;
  SCN rec_scn_;
  SCN end_scn_;
};

static int build_rec_end_sstable_param(const ObTabletID &tablet_id, const SCN &end_scn,
    const int64_t trans_id, const int64_t seq_no, const ObITable::TableType table_type,
    const int64_t upper_trans_version, const bool empty, ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;
  param.table_key_.tablet_id_ = tablet_id;
  param.table_key_.table_type_ = table_type;
  param.table_key_.scn_range_.start_scn_ = SCN::min_scn();
  param.table_key_.scn_range_.end_scn_ = end_scn;
  if (ObITable::is_major_sstable(table_type)) {
    param.table_key_.version_range_.snapshot_version_ = end_scn.get_val_for_tx();
  }
  param.max_merged_trans_version_ = end_scn.get_val_for_tx();
  param.upper_trans_version_ = upper_trans_version;
  param.schema_version_ = 100;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 0;
  param.progressive_merge_step_ = 0;
  param.index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
  param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
  param.table_mode_.pk_mode_ = ObTablePKMode::TPKM_OLD_NO_PK;
  param.table_mode_.state_flag_ = ObTableStateFlag::TABLE_STATE_NORMAL;
  param.rowkey_column_cnt_ = 3;
  param.column_cnt_ = 4;
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  // Dump validity uses the SS DDL start, not base_scn.
  ret = param.ddl_scn_.convert_for_tx(SS_DDL_START_SCN_VAL);
  param.filled_tx_scn_ = end_scn;
  param.rec_scn_ = ObITable::is_major_sstable(table_type)
      ? SCN::min_scn() : SCN::base_scn();
  param.tx_data_recycle_scn_ = SCN::min_scn();
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.column_group_cnt_ = 1;
  param.co_base_type_ = ObCOSSTableBaseType::INVALID_TYPE;
  param.co_base_snapshot_version_ = 0;
  param.sstable_logic_seq_ = 0;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  // These metadata-only fixtures must distinguish nonempty results from empty batches.
  const bool has_data = !empty && !ObITable::is_major_sstable(table_type);
  param.data_blocks_cnt_ = has_data ? 1 : 0;
  param.micro_block_cnt_ = has_data ? 1 : 0;
  param.use_old_macro_block_count_ = 0;
  param.row_count_ = has_data ? 1 : 0;
  param.full_column_cnt_ = 0;
  param.occupy_size_ = 0;
  param.original_size_ = 0;
  param.recycle_version_ = 0;
  param.nested_offset_ = 0;
  param.nested_size_ = 0;
  if (OB_SUCC(ret) && !ObITable::is_major_sstable(table_type)) {
    param.uncommit_tx_info_.set_info_status_record_seq();
    // SSTable metadata stores the encoded transaction sequence, not a plain offset.
    ret = param.uncommit_tx_info_.push_back(compaction::ObUncommitTxDesc(
        trans_id, ObTxSEQ::from_seq(seq_no).cast_to_int()));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(
      param.column_cnt_, param.column_checksums_))) {
  }
  return ret;
}
static int build_rec_end_sstable(ObArenaAllocator &allocator, const ObTabletID &tablet_id,
    const SCN &end_scn, const int64_t trans_id, const int64_t seq_no, ObSSTable &sstable,
    const ObITable::TableType table_type = ObITable::INC_MAJOR_SSTABLE,
    const int64_t upper_trans_version = INT64_MAX, const bool empty = false)
{
  int ret = OB_SUCCESS;
  ObTabletCreateSSTableParam param;
  if (OB_FAIL(build_rec_end_sstable_param(tablet_id, end_scn, trans_id, seq_no,
      table_type, upper_trans_version, empty, param))) {
  } else if (OB_FAIL(sstable.init(param, &allocator))) {
  }
  return ret;
}

class TestDliEndRecCheckpoint : public TestSharedMetaService
{
public:
  // CTest runs both executables concurrently in the same working directory.
  TestDliEndRecCheckpoint()
    : TestSharedMetaService("test_dli_end_rec_checkpoint")
  {}

protected:
  void SetUp() override
  {
    // Real bootstrap and tenant creation must retain normal writer qualifications.
    ATOMIC_STORE(&block_compaction_writer, false);
    TestSharedMetaService::SetUp();
    ASSERT_TRUE(GCTX.is_shared_storage_mode());
    ATOMIC_STORE(&block_compaction_writer, true);
  }

  void TearDown() override
  {
    ATOMIC_STORE(&block_compaction_writer, false);
    TestSharedMetaService::TearDown();
  }

  int prepare(const char *name, RecEndContext &ctx, const bool create_shared = true,
      const char *columns = "a int")
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("create table %s (%s)", name, columns))) {
    } else if (OB_FAIL(exe_sql(sql.ptr()))) {
    } else if (OB_FAIL(get_tablet_and_ls_id(name, ctx.tablet_id_, ctx.ls_id_))) {
    } else if (OB_FAIL(MTL(ObLSService *)->get_ls(
        ctx.ls_id_, ctx.ls_, ObLSGetMod::STORAGE_MOD))) {
    } else if (OB_FAIL(ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_))) {
    } else {
      ctx.reorganization_scn_ = ctx.local_.get_obj()->get_reorganization_scn();
      if (create_shared && OB_FAIL(MTL(ObSSMetaService *)->create_tablet(
          ctx.ls_id_, ctx.tablet_id_, ctx.reorganization_scn_, true))) {
      } else if (OB_FAIL(ctx.local_.get_obj()->load_storage_schema(ctx.allocator_, ctx.schema_))) {
      } else if (OB_FAIL(ctx.rec_scn_.convert_for_tx(ObTimeUtility::current_time_ns()))) {
      } else {
        ctx.end_scn_ = SCN::plus(ctx.rec_scn_, 1000);
      }
    }
    return ret;
  }

  int latest(RecEndContext &ctx, SCN *meta_scn = nullptr)
  {
    ctx.shared_.reset();
    SCN unused;
    return MTL(ObSSMetaService *)->get_tablet(ctx.ls_id_, ctx.tablet_id_,
        ctx.reorganization_scn_, ctx.allocator_, ctx.shared_,
        nullptr == meta_scn ? unused : *meta_scn);
  }

  int publish(RecEndContext &ctx, const ObSSTable &table, const SCN &checkpoint,
      const bool replay, const bool need_check_ddl_checkpoint)
  {
    return ObSSDDLUtil::update_shared_tablet_table_store(*ctx.ls_.get_ls(), table,
        *ctx.schema_, DATA_VERSION_4_6_3_0, ctx.reorganization_scn_, replay, checkpoint,
        need_check_ddl_checkpoint);
  }

  int update_action(RecEndContext &ctx, const ObSSTable &table,
      const ObIncMajorSSTableAction action, const SCN &checkpoint)
  {
    int ret = OB_SUCCESS;
    ObUpdateTableStoreParam param;
    ObArray<ObSSMetaCtrlKeyInfo> keys;
    if (OB_FAIL(ObSSDDLUtil::build_update_table_store_param_(
        *ctx.ls_.get_ls(), table, *ctx.schema_, DATA_VERSION_4_6_3_0, param))) {
    } else {
      param.inc_major_sstable_action_ = action;
      param.ddl_info_.ddl_checkpoint_scn_ = checkpoint;
      ret = MTL(ObSSMetaService *)->update_tablet_table_store(ctx.ls_id_, ctx.tablet_id_,
          ctx.reorganization_scn_, ObMetaUpdateReason::TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE,
          param, keys, true);
    }
    return ret;
  }

  int init_executor(RecEndContext &ctx, const ObSSTable &table, const int64_t tx_id,
      const int64_t seq_no, ObDDLIncMajorCommitReplayExecutor &executor,
      const bool need_check_ddl_checkpoint)
  {
    int ret = OB_SUCCESS;
    ObString buffer;
    if (OB_FAIL(ObIncDDLMergeTaskUtils::serialize_inc_major_to_string(
        ctx.allocator_, &table, DATA_VERSION_4_6_3_0, buffer))) {
    } else {
      ret = executor.init(ctx.ls_.get_ls(), ctx.tablet_id_, ctx.end_scn_,
          ObTransID(tx_id), ObTxSEQ::from_seq(seq_no), ctx.local_.get_obj()->get_snapshot_version(),
          DATA_VERSION_4_6_3_0, false, false, table.get_key().table_type_, buffer, SCN::scn_dec(ctx.rec_scn_),
          need_check_ddl_checkpoint);
    }
    return ret;
  }

  int inc_major_count(const ObTabletHandle &tablet, int64_t &count)
  {
    int ret = OB_SUCCESS;
    ObTabletMemberWrapper<ObTabletTableStore> store;
    if (OB_FAIL(tablet.get_obj()->fetch_table_store_with_prewarm(store))) {
    } else {
      count = store.get_member()->get_inc_major_sstables().count();
    }
    return ret;
  }

  int remove_history_before(RecEndContext &ctx, const SCN &latest_version)
  {
    ObScnRange range;
    range.start_scn_ = SCN::base_scn();
    range.end_scn_ = SCN::scn_dec(latest_version);
    return MTL(ObSSMetaService *)->remove_tablet(
        ctx.ls_id_, ctx.tablet_id_, ctx.reorganization_scn_, range);
  }

  int check_history_removed(RecEndContext &ctx, const SCN &latest_version)
  {
    ObSSMetaReadParamV2 param;
    param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW, ObAtomicProtocolTypeEnum::TABLET_META,
        ctx.ls_id_, ctx.tablet_id_, SCN::min_scn(), ctx.reorganization_scn_, true);
    ObTabletHandle history;
    return MTL(ObSSMetaService *)->get_tablet(param, SCN::scn_dec(latest_version),
        ctx.allocator_, history);
  }

  int record_end_metadata(RecEndContext &ctx, const int64_t tx_id, const SCN &end_scn)
  {
    int ret = OB_SUCCESS;
    ObTabletDDLCompleteArg arg;
    arg.has_complete_ = true;
    arg.ls_id_ = ctx.ls_id_;
    arg.tablet_id_ = ctx.tablet_id_;
    arg.direct_load_type_ = DIRECT_LOAD_INCREMENTAL_MAJOR;
    arg.start_scn_ = SCN::scn_dec(ctx.rec_scn_);
    arg.rec_scn_ = end_scn;
    arg.data_format_version_ = DATA_VERSION_4_6_3_0;
    arg.snapshot_version_ = ctx.local_.get_obj()->get_snapshot_version();
    arg.table_key_.tablet_id_ = ctx.tablet_id_;
    arg.trans_id_ = ObTransID(tx_id);
    if (OB_FAIL(arg.set_storage_schema(*ctx.schema_))) {
    } else {
      ret = ObTabletDDLCompleteMdsHelper::record_ddl_complete_arg_to_mds(arg, ctx.allocator_);
    }
    return ret;
  }

  int seed_transaction(RecEndContext &ctx, const int64_t tx_id,
      const int32_t state, const SCN &end_scn)
  {
    int ret = OB_SUCCESS;
    ObTxDataGuard guard;
    ObTxTable *tx_table = ctx.ls_.get_ls()->get_tx_table();
    if (OB_FAIL(tx_table->alloc_tx_data(guard, false))) {
    } else {
      ObTxData *data = guard.tx_data();
      data->tx_id_ = ObTransID(tx_id);
      data->start_scn_ = SCN::scn_dec(ctx.rec_scn_);
      data->end_scn_ = end_scn;
      data->state_ = state;
      if (OB_FAIL(data->commit_version_.convert_for_tx(
          ObTxData::COMMIT == state ? end_scn.get_val_for_tx() : 0))) {
      } else {
        ret = tx_table->insert(data);
      }
    }
    return ret;
  }

  void check_lob_end_retry(const bool empty_lob_piece);

#ifdef ERRSIM
  int prepare_full_inc_kv_queue(RecEndContext &ctx, const ObTransID &trans_id,
      ObDDLKvMgrHandle &mgr, ObDDLKVHandle &first, ObDDLTableMergeDag &dag, SCN &end_scn)
  {
    int ret = OB_SUCCESS;
    SCN start_scn;
    const int64_t snapshot = ctx.local_.get_obj()->get_snapshot_version();
    const ObTxSEQ seq_no = ObTxSEQ::from_seq(1);
    // Only fixture construction suppresses scheduling; the tested retries submit real DAGs.
    NamedEventGuard setup_only;
    if (OB_FAIL(start_scn.convert_for_tx(SS_DDL_START_SCN_VAL))) {
    } else if (OB_FAIL(ObDDLIncLoadUtil::record_inc_major_start_info_to_mds(*ctx.schema_,
        ctx.tablet_id_, ctx.ls_id_, trans_id, DATA_VERSION_4_6_3_0, snapshot, start_scn, ctx.allocator_))) {
    } else if (OB_FAIL(ctx.local_.get_obj()->get_ddl_kv_mgr(mgr, true))) {
    } else if (OB_FAIL(ctx.ls_.get_ls()->get_max_decided_scn(end_scn))) {
    } else if (FALSE_IT(end_scn = SCN::minus(end_scn, 1000))) {
    } else if (OB_FAIL(mgr.get_obj()->set_max_freeze_scn(end_scn))) {
    } else if (OB_FAIL(setup_only.init("EN_COMPACTION_DISABLE_INC_MAJOR_SSTABLE", OB_EAGAIN, 100))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ObTabletDDLKvMgr::MAX_DDL_KV_CNT_IN_STORAGE; ++i) {
      ObDDLKVHandle kv;
      end_scn = SCN::plus(end_scn, 1);
      if (OB_FAIL(mgr.get_obj()->get_or_create_inc_major_ddl_kv(end_scn, start_scn, snapshot,
          DATA_VERSION_4_6_3_0, trans_id, seq_no, ObITable::INC_MAJOR_DDL_MEM_SSTABLE, kv))) {
      } else {
        // Metadata-only KVs isolate queue draining from macro-block I/O.
        kv.get_obj()->min_scn_ = kv.get_obj()->max_scn_ = end_scn;
        if (i == 0) {
          first = kv;
        }
        ret = mgr.get_obj()->freeze_ddl_kv(start_scn, snapshot, DATA_VERSION_4_6_3_0,
            end_scn, ObDDLKVType::DDL_KV_INC_MAJOR, trans_id, seq_no, ObITable::INC_MAJOR_DDL_MEM_SSTABLE);
      }
    }
    if (OB_SUCC(ret)) {
      ObDDLTableMergeDagParam param;
      param.direct_load_type_ = DIRECT_LOAD_INCREMENTAL_MAJOR;
      param.ls_id_ = ctx.ls_id_;
      param.tablet_id_ = ctx.tablet_id_;
      param.start_scn_ = first.get_obj()->get_start_scn();
      param.snapshot_version_ = snapshot;
      param.data_format_version_ = DATA_VERSION_4_6_3_0;
      param.trans_id_ = trans_id;
      param.seq_no_ = seq_no;
      param.table_type_ = ObITable::INC_MAJOR_DDL_DUMP_SSTABLE;
      ret = dag.init_by_param(&param);
    }
    return ret;
  }

  template <typename Predicate>
  bool wait_for_inc_kv_condition(Predicate predicate)
  {
    const int64_t deadline = ObTimeUtility::current_time() + 30 * 1000 * 1000;
    while (!predicate()) {
      if (ObTimeUtility::current_time() >= deadline) {
        return false;
      }
      ob_usleep(1000);
    }
    return true;
  }

  bool inc_kv_dag_finished(const ObDDLTableMergeDag &dag)
  {
    bool exists = true;
    bool emergency = false;
    int ret = MTL(ObTenantDagScheduler *)->check_dag_exist(&dag, exists, emergency);
    return OB_SUCC(ret) && !exists;
  }

  void check_inc_kv_dump(RecEndContext &ctx, const ObTransID &trans_id, const SCN &end_scn)
  {
    ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
    ObTableStoreIterator dumps;
    ASSERT_EQ(OB_SUCCESS, ctx.local_.get_obj()->get_inc_major_ddl_sstables(
        dumps, trans_id, ObTxSEQ::from_seq(1)));
    ASSERT_EQ(1, dumps.count());
    ObITable *table = nullptr;
    ASSERT_EQ(OB_SUCCESS, dumps.get_next(table));
    ASSERT_NE(nullptr, table);
    EXPECT_EQ(end_scn, table->get_end_scn());
    EXPECT_EQ(end_scn, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
  }

#endif
};

TEST_F(TestDliEndRecCheckpoint, dump_checkpoint_does_not_skip_final_publication)
{
  // A real Dump update may reach rec_scn before a final IncMajor exists.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_dump", ctx));
  ASSERT_EQ(OB_SUCCESS, latest(ctx));
  ObSSTable dump;
  ObSSTable final_table;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77001, 1, dump, ObITable::INC_MAJOR_DDL_DUMP_SSTABLE));
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77001, 1, final_table));
  ObMiniSSTableUploadTask uploader;
  uploader.param_.ls_id_ = ctx.ls_id_;
  ASSERT_EQ(OB_SUCCESS, uploader.param_.snapshot_version_.convert_for_tx(
      ctx.local_.get_obj()->get_snapshot_version()));
  ObUpdateTableStoreParam param;
  ASSERT_EQ(OB_SUCCESS, uploader.build_update_table_store_param_(ctx.shared_,
      *ctx.local_.get_obj(), dump, *ctx.schema_, DATA_VERSION_4_6_3_0, false,
      ctx.allocator_, param));
  ASSERT_EQ(ctx.rec_scn_, param.ddl_info_.ddl_checkpoint_scn_);
  ObTabletHandle installed_local;
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->update_tablet_table_store(
      ctx.tablet_id_, param, installed_local));
  ObDDLIncLogBasic batch_key;
  ASSERT_EQ(OB_SUCCESS, batch_key.init(ctx.tablet_id_, ObTabletID(), ObTabletID(),
      DIRECT_LOAD_INCREMENTAL_MAJOR, ObTransID(77001), ObTxSEQ::from_seq(1),
      ctx.local_.get_obj()->get_snapshot_version(), DATA_VERSION_4_6_3_0));
  // Keep the cached LS watermark independent of background upload scheduling.
  ObLSIncSSTableUploader checkpoint_uploader(*ctx.ls_.get_ls());
  (void)checkpoint_uploader.ckpt_scn_on_ss_ls_meta_.atomic_store(SCN::scn_dec(ctx.rec_scn_));
  checkpoint_uploader.is_online_ = true;
  checkpoint_uploader.finish_reloading_ = true;
  // DLI-CACHED-READ-001: observe only this thread inside each readiness call.
  lib::ObUnitTestEnableDiagnoseGuard enable_diagnose_guard;
  const bool old_diagnose = lib::ObLibConfig::enable_diagnose_info_;
  lib::reload_diagnose_info_config(true);
  DEFER(lib::reload_diagnose_info_config(old_diagnose));
  ASSERT_TRUE(lib::is_diagnose_info_enabled());
  ObDiagnosticInfoSlot read_stats;
  ObStatEventAddStat *read_count = read_stats.get_stats().get(ObStatEventIds::SS_META_READ_COUNT);
  ASSERT_NE(nullptr, read_count);
  // Calibrate with a successful Shared read so a disabled counter cannot pass.
  {
    ObTenantDiagnosticInfoSummaryGuard read_guard(&read_stats);
    ASSERT_EQ(OB_SUCCESS, latest(ctx));
  }
  ASSERT_GT(read_count->stat_value_, 0);
  auto check_dump_ready_without_shared_reads = [&](bool &ready) {
    const int64_t reads_before = read_count->stat_value_;
    int ret = OB_SUCCESS;
    {
      ObTenantDiagnosticInfoSummaryGuard read_guard(&read_stats);
      ret = checkpoint_uploader.check_inc_major_dump_ready(batch_key, ready);
    }
    EXPECT_EQ(0, read_count->stat_value_ - reads_before)
        << "dump readiness must not synchronously read Shared metadata";
    return ret;
  };
  bool ready = false;
  ASSERT_EQ(OB_SUCCESS, check_dump_ready_without_shared_reads(ready));
  ASSERT_FALSE(ready); // The local dump has not reached Shared yet.
  ObArray<ObSSMetaCtrlKeyInfo> keys;
  ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->update_tablet_table_store(ctx.ls_id_,
      ctx.tablet_id_, ctx.reorganization_scn_,
      ObMetaUpdateReason::TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE, param, keys, true));
  ASSERT_EQ(OB_SUCCESS, latest(ctx));
  int64_t count = -1;
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.shared_, count));
  ASSERT_EQ(0, count);
  ASSERT_EQ(ctx.rec_scn_, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  ASSERT_LT(ctx.shared_.get_obj()->get_ddl_checkpoint_scn(), ctx.end_scn_);
  ASSERT_EQ(OB_SUCCESS, check_dump_ready_without_shared_reads(ready));
  ASSERT_FALSE(ready); // Shared DDL progress alone does not advance the cached LS watermark.
  // Repeated checks must return while the cache stays behind. This measures the
  // hot local check, not background checkpoint advancement or a production SLA.
  const int64_t check_count = 100;
  int64_t max_check_us = 0;
  const int64_t checks_started = ObTimeUtility::current_monotonic_time();
  for (int64_t i = 0; i < check_count; ++i) {
    const int64_t started = ObTimeUtility::current_monotonic_time();
    ASSERT_EQ(OB_SUCCESS, check_dump_ready_without_shared_reads(ready));
    const int64_t elapsed_us = ObTimeUtility::current_monotonic_time() - started;
    max_check_us = MAX(max_check_us, elapsed_us);
    ASSERT_FALSE(ready);
  }
  const int64_t total_check_us = ObTimeUtility::current_monotonic_time() - checks_started;
  RecordProperty("cached_checkpoint_pending_checks", static_cast<int>(check_count));
  RecordProperty("cached_checkpoint_checks_total_us", static_cast<int>(total_check_us));
  RecordProperty("cached_checkpoint_check_max_us", static_cast<int>(max_check_us));
  // The fixture exits before GTest writes XML, so retain timing in stdout too.
  fprintf(stdout, "DLI_CACHED_CHECKPOINT_TIMING count=%ld total_us=%ld max_us=%ld\n",
      check_count, total_check_us, max_check_us);
  fflush(stdout);
  (void)checkpoint_uploader.ckpt_scn_on_ss_ls_meta_.atomic_store(ctx.rec_scn_);
  ASSERT_EQ(OB_SUCCESS, check_dump_ready_without_shared_reads(ready));
  ASSERT_TRUE(ready);
  (void)checkpoint_uploader.ckpt_scn_on_ss_ls_meta_.atomic_store(SCN::scn_inc(ctx.rec_scn_));
  ASSERT_EQ(OB_SUCCESS, check_dump_ready_without_shared_reads(ready));
  ASSERT_TRUE(ready);
  ObTableStoreIterator installed_dumps;
  ASSERT_EQ(OB_SUCCESS, ctx.shared_.get_obj()->get_inc_major_ddl_sstables(
      installed_dumps, ObTransID(77001), ObTxSEQ::from_seq(1)));
  ASSERT_EQ(1, installed_dumps.count());
  ObDDLIncMajorCommitReplayExecutor executor;
  ASSERT_EQ(OB_SUCCESS, init_executor(ctx, final_table, 77001, 1, executor, true));
  RecEndRoleGuard role_guard;
  MTL_SET_TENANT_ROLE_CACHE(share::ObTenantRole::STANDBY_TENANT);
  mock_is_sswriter = false;
  bool need_publish = false;
  ASSERT_EQ(OB_NOT_MASTER, executor.deserialize_and_update_ss_inc_major(ctx.local_, need_publish));
  mock_is_sswriter = true;
  ASSERT_EQ(OB_SUCCESS, executor.deserialize_and_update_ss_inc_major(ctx.local_, need_publish));
  ASSERT_TRUE(need_publish);
  SCN shared_meta_before;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &shared_meta_before));
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.shared_, count));
  ASSERT_EQ(1, count);
  ASSERT_EQ(ctx.end_scn_, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  // Exact END coverage skips Shared writes while the follower still completes Local LINK.
  mock_is_sswriter = false;
  ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
  ASSERT_GE(ctx.local_.get_obj()->get_ddl_checkpoint_scn(), ctx.end_scn_);
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.local_, count));
  ASSERT_EQ(1, count);
  SCN shared_meta_after;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &shared_meta_after));
  ASSERT_EQ(shared_meta_before, shared_meta_after);
  role_guard.restore();
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_dump"));
}

TEST_F(TestDliEndRecCheckpoint, link_installs_table_and_checkpoint_together)
{
  // The wrapper installs the table and its checkpoint in the same Local update.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_link_checkpoint", ctx, true, "a int"));
  ObSSTable table;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77310, 1, table, ObITable::INC_MAJOR_SSTABLE, INT64_MAX, false));
  ASSERT_EQ(OB_SUCCESS, publish(ctx, table, ctx.rec_scn_, false, false));
  // Establish a private persisted version before measuring this wrapper's Local writes.
  ObUpdateTableStoreParam baseline(ctx.local_.get_obj()->get_snapshot_version(),
      ObVersionRange::MIN_VERSION, ctx.schema_, ctx.ls_.get_ls()->get_rebuild_seq(),
      nullptr, false, true);
  baseline.ddl_info_.ddl_checkpoint_scn_ = ctx.local_.get_obj()->get_ddl_checkpoint_scn();
  ObTabletHandle baseline_tablet;
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->update_tablet_table_store(
      ctx.tablet_id_, baseline, baseline_tablet));
  MacroBlockId before_id;
  ASSERT_EQ(OB_SUCCESS, baseline_tablet.get_obj()->get_tablet_addr().get_macro_block_id(before_id));
  ASSERT_EQ(OB_SUCCESS, ObIncDDLMergeTaskUtils::link_inc_major(
      ctx.ls_.get_ls(), baseline_tablet, ObTransID(77310), ObTxSEQ::from_seq(1), ctx.end_scn_));
  ObTabletHandle linked;
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(
      ctx.tablet_id_, linked, ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT,
      ObMDSGetTabletMode::READ_ALL_COMMITED));
  ASSERT_TRUE(linked.is_valid());
  ASSERT_NE(baseline_tablet.get_obj(), linked.get_obj());
  // Two successful writes (LINK then checkpoint) consume two private metadata versions.
  MacroBlockId after_id;
  ASSERT_EQ(OB_SUCCESS, linked.get_obj()->get_tablet_addr().get_macro_block_id(after_id));
  ASSERT_EQ(before_id.meta_version_id() + 1, after_id.meta_version_id());
  ASSERT_EQ(ctx.end_scn_, linked.get_obj()->get_ddl_checkpoint_scn());
  int64_t count = -1;
  ASSERT_EQ(OB_SUCCESS, inc_major_count(linked, count));
  ASSERT_EQ(1, count);
  ASSERT_EQ(OB_SUCCESS, latest(ctx, nullptr));
  ASSERT_EQ(ctx.rec_scn_, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_link_checkpoint"));
}

TEST_F(TestDliEndRecCheckpoint, existing_local_table_still_advances_end_checkpoint)
{
  // An already linked data result still needs its later END checkpoint persisted.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_link_existing", ctx, true, "a int"));
  ObSSTable table;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77311, 1, table, ObITable::INC_MAJOR_SSTABLE, INT64_MAX, false));
  ASSERT_EQ(OB_SUCCESS, publish(ctx, table, ctx.rec_scn_, false, false));
  bool is_linked = false;
  ASSERT_EQ(OB_SUCCESS, share::SSCompactHelper::link_inc_major(
      ctx.ls_id_, ctx.tablet_id_, ctx.local_.get_obj()->get_snapshot_version(),
      ObTransID(77311), ObTxSEQ::from_seq(1), ctx.rec_scn_, is_linked));
  ASSERT_TRUE(is_linked);
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(
      ctx.tablet_id_, ctx.local_, ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT,
      ObMDSGetTabletMode::READ_ALL_COMMITED));
  ASSERT_EQ(ctx.rec_scn_, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
  ObTabletHandle before_checkpoint;
  ASSERT_EQ(OB_SUCCESS, before_checkpoint.assign(ctx.local_));
  const ObMetaDiskAddr table_store_addr = before_checkpoint.get_obj()->get_table_store_addr();
  const ObMetaDiskAddr schema_addr = before_checkpoint.get_obj()->get_storage_schema_addr();
  const ObMetaDiskAddr tablet_addr = before_checkpoint.get_obj()->get_tablet_addr();
  ASSERT_FALSE(table_store_addr.is_none());
  const int64_t rebuild_seq = ctx.ls_.get_ls()->get_rebuild_seq();
  const int64_t transfer_seq = before_checkpoint.get_obj()->get_transfer_seq();
  // An obsolete rebuild or transfer generation must not publish the checkpoint.
  ASSERT_EQ(OB_EAGAIN, ctx.ls_.get_ls()->update_tablet_ddl_checkpoint(
      ctx.tablet_id_, rebuild_seq + 1, transfer_seq, ctx.end_scn_));
  ASSERT_EQ(OB_TABLET_TRANSFER_SEQ_NOT_MATCH,
      ctx.ls_.get_ls()->update_tablet_ddl_checkpoint(
          ctx.tablet_id_, rebuild_seq, transfer_seq + 1, ctx.end_scn_));
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_,
      ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT, ObMDSGetTabletMode::READ_ALL_COMMITED));
  ASSERT_EQ(tablet_addr, ctx.local_.get_obj()->get_tablet_addr());
  ASSERT_EQ(ctx.rec_scn_, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
  // Skipped LINK persists only metadata and keeps the TableStore and schema objects.
  RecEndRoleGuard role_guard;
  mock_is_sswriter = false;
  ASSERT_EQ(OB_SUCCESS, ObIncDDLMergeTaskUtils::link_inc_major(
      ctx.ls_.get_ls(), ctx.local_, ObTransID(77311), ObTxSEQ::from_seq(1), ctx.end_scn_));
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(
      ctx.tablet_id_, ctx.local_, ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT,
      ObMDSGetTabletMode::READ_ALL_COMMITED));
  ASSERT_EQ(ctx.end_scn_, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
  ASSERT_EQ(table_store_addr, ctx.local_.get_obj()->get_table_store_addr());
  ASSERT_EQ(schema_addr, ctx.local_.get_obj()->get_storage_schema_addr());
  ASSERT_NE(tablet_addr, ctx.local_.get_obj()->get_tablet_addr());
  const ObMetaDiskAddr completed_addr = ctx.local_.get_obj()->get_tablet_addr();
  // A stale caller must observe the latest checkpoint and avoid another metadata write.
  ASSERT_EQ(OB_SUCCESS, ObIncDDLMergeTaskUtils::link_inc_major(ctx.ls_.get_ls(),
      before_checkpoint, ObTransID(77311), ObTxSEQ::from_seq(1), ctx.rec_scn_));
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_,
      ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT, ObMDSGetTabletMode::READ_ALL_COMMITED));
  ASSERT_EQ(ctx.end_scn_, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
  ASSERT_EQ(completed_addr, ctx.local_.get_obj()->get_tablet_addr());
  ObDDLIncMajorCommitReplayExecutor executor;
  ASSERT_EQ(OB_SUCCESS, init_executor(ctx, table, 77311, 1, executor, true));
  ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(
      ctx.tablet_id_, ctx.local_, ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT,
      ObMDSGetTabletMode::READ_ALL_COMMITED));
  ASSERT_EQ(ctx.end_scn_, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
  int64_t count = -1;
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.local_, count));
  ASSERT_EQ(1, count);
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_link_existing"));
}

TEST_F(TestDliEndRecCheckpoint, dump_checkpoint_does_not_skip_atomic_add)
{
  // Foreground ADD must publish the final table while its matching Dump remains.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_dump_atomic", ctx, true, "a int"));
  ASSERT_EQ(OB_SUCCESS, latest(ctx, nullptr));
  RecEndRoleGuard role_guard;
  mock_is_sswriter = true;
  ObSSTable dump;
  ObSSTable final_table;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77306, 1, dump, ObITable::INC_MAJOR_DDL_DUMP_SSTABLE,
      INT64_MAX, false));
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77306, 1, final_table, ObITable::INC_MAJOR_SSTABLE,
      INT64_MAX, false));
  ObMiniSSTableUploadTask uploader;
  uploader.param_.ls_id_ = ctx.ls_id_;
  ASSERT_EQ(OB_SUCCESS, uploader.param_.snapshot_version_.convert_for_tx(
      ctx.local_.get_obj()->get_snapshot_version()));
  ObUpdateTableStoreParam param;
  ASSERT_EQ(OB_SUCCESS, uploader.build_update_table_store_param_(ctx.shared_,
      *ctx.local_.get_obj(), dump, *ctx.schema_, DATA_VERSION_4_6_3_0, false,
      ctx.allocator_, param));
  ObArray<ObSSMetaCtrlKeyInfo> keys;
  ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->update_tablet_table_store(
      ctx.ls_id_, ctx.tablet_id_, ctx.reorganization_scn_,
      ObMetaUpdateReason::TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE, param, keys, true));
  ASSERT_EQ(OB_SUCCESS, latest(ctx, nullptr));
  ASSERT_EQ(ctx.rec_scn_, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  {
    ObTableStoreIterator installed_dumps;
    ASSERT_EQ(OB_SUCCESS, ctx.shared_.get_obj()->get_inc_major_ddl_sstables(
        installed_dumps, ObTransID(77306), ObTxSEQ::from_seq(1)));
    ASSERT_EQ(1, installed_dumps.count());
  }
  ASSERT_EQ(OB_SUCCESS, publish(
      ctx, final_table, final_table.get_end_scn(), false, false));
  ASSERT_EQ(OB_SUCCESS, latest(ctx, nullptr));
  int64_t count = -1;
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.shared_, count));
  ASSERT_EQ(1, count);
  ASSERT_EQ(final_table.get_end_scn(), ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  {
    ObTableStoreIterator remaining_dumps;
    ASSERT_EQ(OB_SUCCESS, ctx.shared_.get_obj()->get_inc_major_ddl_sstables(
        remaining_dumps, ObTransID(77306), ObTxSEQ::from_seq(1)));
    ASSERT_EQ(0, remaining_dumps.count());
  }
  role_guard.restore();
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_dump_atomic"));
}

TEST_F(TestDliEndRecCheckpoint, earlier_batch_dump_does_not_hide_final_publication)
{
  // An older aborted batch's Dump is not an unfinished result of the current batch.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_other_dump", ctx, true, "a int"));
  ASSERT_EQ(OB_SUCCESS, latest(ctx, nullptr));
  ObSSTable dump;
  ObSSTable final_table;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      SCN::scn_dec(ctx.rec_scn_), 77302, 1, dump,
      ObITable::INC_MAJOR_DDL_DUMP_SSTABLE, INT64_MAX, false));
  ObMiniSSTableUploadTask uploader;
  uploader.param_.ls_id_ = ctx.ls_id_;
  ASSERT_EQ(OB_SUCCESS, uploader.param_.snapshot_version_.convert_for_tx(
      ctx.local_.get_obj()->get_snapshot_version()));
  ObUpdateTableStoreParam param;
  ASSERT_EQ(OB_SUCCESS, uploader.build_update_table_store_param_(ctx.shared_,
      *ctx.local_.get_obj(), dump, *ctx.schema_, DATA_VERSION_4_6_3_0, false,
      ctx.allocator_, param));
  ObArray<ObSSMetaCtrlKeyInfo> keys;
  ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->update_tablet_table_store(
      ctx.ls_id_, ctx.tablet_id_, ctx.reorganization_scn_,
      ObMetaUpdateReason::TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE, param, keys, true));
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77303, 1, final_table, ObITable::INC_MAJOR_SSTABLE,
      INT64_MAX, false));
  ASSERT_EQ(OB_SUCCESS, publish(ctx, final_table, ctx.rec_scn_, false, false));
  SCN before;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &before));
  ObTabletMemberWrapper<ObTabletTableStore> store;
  ASSERT_EQ(OB_SUCCESS, ctx.shared_.get_obj()->fetch_table_store_with_prewarm(store));
  ASSERT_EQ(1, store.get_member()->get_inc_major_ddl_sstables().count());
  ObDDLIncMajorCommitReplayExecutor executor;
  ASSERT_EQ(OB_SUCCESS, init_executor(ctx, final_table, 77303, 1, executor,
      true));
  RecEndRoleGuard role_guard;
  mock_is_sswriter = false;
  ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
  SCN after;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &after));
  ASSERT_EQ(before, after);
  ASSERT_EQ(ctx.rec_scn_, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  role_guard.restore();
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_other_dump"));
}

TEST_F(TestDliEndRecCheckpoint, committed_result_keeps_its_publication_checkpoint)
{
  // Replayed ADD preserves committed metadata without a second Shared checkpoint update.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_committed", ctx));
  ObSSTable original;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77002, 2, original));
  ASSERT_EQ(OB_SUCCESS, publish(ctx, original, ctx.end_scn_, true, true));
  ObSSTable *committed = nullptr;
  ASSERT_EQ(OB_SUCCESS, original.deep_copy(ctx.allocator_, committed));
  const int64_t commit_version = SCN::plus(ctx.end_scn_, 100).get_val_for_tx();
  ASSERT_EQ(OB_SUCCESS, committed->backfill_commit_version(
      ctx.allocator_, commit_version, SCN::plus(ctx.end_scn_, 200)));
  ASSERT_EQ(OB_SUCCESS, update_action(ctx, *committed,
      ObIncMajorSSTableAction::COMMIT, ctx.rec_scn_));
  SCN before;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &before));
  SCN after;
  ObDDLIncMajorCommitReplayExecutor executor;
  ASSERT_EQ(OB_SUCCESS, init_executor(ctx, original, 77002, 2, executor, true));
  RecEndRoleGuard role_guard;
  mock_is_sswriter = false;
  ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &after));
  ASSERT_EQ(before, after);
  mock_is_sswriter = true;
  ASSERT_EQ(OB_SUCCESS, publish(ctx, original, ctx.end_scn_, true, true));
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &after));
  ASSERT_EQ(after, before);
  ASSERT_EQ(ctx.end_scn_, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  ObTabletMemberWrapper<ObTabletTableStore> store;
  ASSERT_EQ(OB_SUCCESS, ctx.shared_.get_obj()->fetch_table_store_with_prewarm(store));
  ASSERT_EQ(1, store.get_member()->get_inc_major_sstables().count());
  const ObSSTable *result = store.get_member()->get_inc_major_sstables().at(0);
  ASSERT_EQ(commit_version, result->get_upper_trans_version());
  ASSERT_EQ(committed->get_filled_tx_scn(), result->get_filled_tx_scn());
  ASSERT_EQ(OB_SUCCESS, publish(ctx, original, ctx.end_scn_, true, true));
  SCN duplicate;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &duplicate));
  ASSERT_EQ(after, duplicate);
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
  ASSERT_GE(ctx.local_.get_obj()->get_ddl_checkpoint_scn(), ctx.end_scn_);
  ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
  role_guard.restore();
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_committed"));
}

TEST_F(TestDliEndRecCheckpoint, old_format_end_maintains_shared_and_local_progress)
{
  // Legacy END records progress without enabling the new checkpoint-based ADD filter.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_old_progress", ctx, true, "a int"));
  ObSSTable table;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77305, 1, table, ObITable::INC_MAJOR_SSTABLE, INT64_MAX, false));
  ObDDLIncMajorCommitReplayExecutor executor;
  ASSERT_EQ(OB_SUCCESS, init_executor(ctx, table, 77305, 1, executor,
      false));
  RecEndRoleGuard role_guard;
  MTL_SET_TENANT_ROLE_CACHE(share::ObTenantRole::STANDBY_TENANT);
  mock_is_sswriter = false;
  ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
  SCN before;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &before));
  ASSERT_EQ(ctx.end_scn_, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  ASSERT_EQ(DATA_VERSION_4_6_3_0,
      ctx.shared_.get_obj()->get_tablet_meta().ddl_data_format_version_);
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_,
      ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US, ObMDSGetTabletMode::READ_READABLE_COMMITED));
  ASSERT_EQ(ctx.end_scn_, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
  // Legacy duplicate ADD keeps the baseline error without advancing its checkpoint.
  const SCN later_scn = SCN::plus(ctx.end_scn_, 100);
  mock_is_sswriter = true;
  ASSERT_EQ(OB_SSTABLE_EXIST, publish(ctx, table, later_scn, true, false));
  SCN after;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &after));
  ASSERT_EQ(before, after);
  ASSERT_EQ(ctx.end_scn_, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  ASSERT_EQ(DATA_VERSION_4_6_3_0,
      ctx.shared_.get_obj()->get_tablet_meta().ddl_data_format_version_);
  role_guard.restore();
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_old_progress"));
}

TEST_F(TestDliEndRecCheckpoint, abort_skips_removed_history_and_advances_local_checkpoint)
{
  // An aborted result stays absent even after its historical metadata has been deleted.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_abort", ctx));
  ObSSTable table;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77003, 3, table));
  // The previous writer completed this END before transaction cleanup.
  ASSERT_EQ(OB_SUCCESS, publish(ctx, table, ctx.end_scn_, true, true));
  const SCN safe_scn = ctx.end_scn_;
  ASSERT_EQ(OB_SUCCESS, update_action(ctx, table, ObIncMajorSSTableAction::ABORT, SCN::min_scn()));
  SCN removed_version;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &removed_version));
  // Enter the RMW directly to cover cleanup between the readiness read and the atomic ADD.
  ASSERT_EQ(OB_SUCCESS, publish(ctx, table, ctx.end_scn_, true, true));
  SCN retry_version;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &retry_version));
  ASSERT_EQ(removed_version, retry_version);
  int64_t count = -1;
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.shared_, count));
  ASSERT_EQ(0, count);
  ASSERT_EQ(OB_SUCCESS, remove_history_before(ctx, removed_version));
  ASSERT_EQ(OB_OBJECT_NOT_EXIST, check_history_removed(ctx, removed_version));
  ObDDLIncMajorCommitReplayExecutor executor;
  ASSERT_EQ(OB_SUCCESS, init_executor(ctx, table, 77003, 3, executor, true));
  RecEndRoleGuard role_guard;
  mock_is_sswriter = false;
  ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.local_, count));
  ASSERT_EQ(0, count);
  ASSERT_GE(ctx.local_.get_obj()->get_ddl_checkpoint_scn(), ctx.end_scn_);
  ASSERT_EQ(OB_SUCCESS, latest(ctx));
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.shared_, count));
  ASSERT_EQ(0, count);
  ASSERT_EQ(safe_scn, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
  role_guard.restore();
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_abort"));
}

TEST_F(TestDliEndRecCheckpoint, major_cleanup_skips_old_end_then_links_covering_major)
{
  // END advances without an input table; the later Major LINK installs the covering result.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_major", ctx));
  ObSSTable table;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77004, 4, table));
  ASSERT_EQ(OB_SUCCESS, publish(ctx, table, ctx.rec_scn_, false, false));
  const SCN commit_scn = SCN::plus(ctx.end_scn_, 100);
  const SCN safe_scn = ctx.rec_scn_;
  ObSSTable *committed = nullptr;
  ASSERT_EQ(OB_SUCCESS, table.deep_copy(ctx.allocator_, committed));
  ASSERT_EQ(OB_SUCCESS, committed->set_upper_trans_version(
      ctx.allocator_, commit_scn.get_val_for_tx()));
  ASSERT_EQ(OB_SUCCESS, update_action(ctx, *committed,
      ObIncMajorSSTableAction::COMMIT, SCN::min_scn()));
  ObSSTable major;
  const SCN major_scn = SCN::plus(commit_scn, 50);
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_, major_scn,
      0, 0, major, ObITable::MAJOR_SSTABLE, major_scn.get_val_for_tx()));
  ASSERT_EQ(OB_SUCCESS, publish(ctx, major, SCN::min_scn(), false, false));
  SCN major_meta_scn;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &major_meta_scn));
  ASSERT_EQ(OB_SUCCESS, remove_history_before(ctx, major_meta_scn));
  ASSERT_EQ(OB_OBJECT_NOT_EXIST, check_history_removed(ctx, major_meta_scn));
  int64_t count = -1;
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.shared_, count));
  ASSERT_EQ(0, count);
  ASSERT_EQ(safe_scn, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  ObDDLIncMajorCommitReplayExecutor executor;
  ASSERT_EQ(OB_SUCCESS, init_executor(ctx, table, 77004, 4, executor, true));
  RecEndRoleGuard role_guard;
  mock_is_sswriter = false;
  ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.local_, count));
  ASSERT_EQ(0, count);
  ASSERT_EQ(ctx.end_scn_, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
  // The normal compaction consumer can install the result after DLI END has completed.
  ASSERT_EQ(OB_SUCCESS, share::SSCompactHelper::link_major(ctx.ls_id_, ctx.tablet_id_,
      compaction::MEDIUM_MERGE, major_scn.get_val_for_tx(), 0, false, false));
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.local_, count));
  ASSERT_EQ(0, count);
  ASSERT_GE(ctx.local_.get_obj()->get_last_major_snapshot_version(), major_scn.get_val_for_tx());
  ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.local_, count));
  ASSERT_EQ(0, count);
  role_guard.restore();
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_major"));
}

TEST_F(TestDliEndRecCheckpoint, inc_major_cleanup_links_later_compaction_without_inputs)
{
  // Under either tenant role, direct LINK installs the output after old END skips LINK.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  const char *names[] = {"rec_end_inc_primary", "rec_end_inc_standby"};
  for (int64_t mode = 0; mode < 2; ++mode) {
    RecEndContext ctx;
    ASSERT_EQ(OB_SUCCESS, prepare(names[mode], ctx, true, "a int"));
    ctx.schema_->index_type_ = ObIndexType::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL;
    ASSERT_EQ(OB_SUCCESS,
        ObIncDDLMergeTaskUtils::update_tablet_table_store_with_storage_schema(
            ctx.ls_.get_ls(), ctx.local_, ctx.schema_));
    ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
    const int64_t base_version = ctx.rec_scn_.get_val_for_tx() - 100;
    const int64_t snapshot = ctx.rec_scn_.get_val_for_tx() + 2000;
    const int64_t tx_id = 77200 + mode;
    ObTabletCreateSSTableParam table_param;
    ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable_param(ctx.tablet_id_, ctx.rec_scn_, tx_id, 1,
        ObITable::INC_MAJOR_SSTABLE, INT64_MAX, false, table_param));
    table_param.index_type_ = ObIndexType::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL;
    table_param.min_merged_trans_version_ = base_version;
    ObSSTable original;
    ASSERT_EQ(OB_SUCCESS, original.init(table_param, &ctx.allocator_));
    ASSERT_EQ(OB_SUCCESS, publish(ctx, original, ctx.rec_scn_, false, false));
    ObSSTable *committed = nullptr;
    ASSERT_EQ(OB_SUCCESS, original.deep_copy(ctx.allocator_, committed));
    const SCN commit_scn = SCN::plus(ctx.end_scn_, 100);
    ASSERT_EQ(OB_SUCCESS, committed->set_upper_trans_version(
        ctx.allocator_, commit_scn.get_val_for_tx()));
    ASSERT_EQ(OB_SUCCESS, update_action(ctx, *committed,
        ObIncMajorSSTableAction::COMMIT, SCN::min_scn()));
    // A second committed refresh supplies the output identity of the later compaction.
    const SCN second_data_scn = SCN::plus(ctx.rec_scn_, 2000);
    const SCN second_commit_scn = SCN::plus(ctx.end_scn_, 2100);
    ObTabletCreateSSTableParam second_param;
    ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable_param(ctx.tablet_id_, second_data_scn,
        tx_id + 100, 1, ObITable::INC_MAJOR_SSTABLE, INT64_MAX, false, second_param));
    second_param.index_type_ = ObIndexType::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL;
    second_param.min_merged_trans_version_ = ctx.rec_scn_.get_val_for_tx();
    ObSSTable second;
    ASSERT_EQ(OB_SUCCESS, second.init(second_param, &ctx.allocator_));
    ASSERT_EQ(OB_SUCCESS, publish(ctx, second, second_data_scn, false, false));
    ObSSTable *second_committed = nullptr;
    ASSERT_EQ(OB_SUCCESS, second.deep_copy(ctx.allocator_, second_committed));
    ASSERT_EQ(OB_SUCCESS, second_committed->set_upper_trans_version(
        ctx.allocator_, second_commit_scn.get_val_for_tx()));
    ASSERT_EQ(OB_SUCCESS, update_action(ctx, *second_committed,
        ObIncMajorSSTableAction::COMMIT, SCN::min_scn()));
    ObTabletCreateSSTableParam output_param;
    ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable_param(ctx.tablet_id_, second_data_scn,
        tx_id + 100, 1, ObITable::INC_MAJOR_SSTABLE,
        second_commit_scn.get_val_for_tx(), false, output_param));
    output_param.index_type_ = ObIndexType::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL;
    output_param.min_merged_trans_version_ = base_version;
    output_param.table_key_.scn_range_.start_scn_ = original.get_start_scn();
    ObSSTable output;
    ASSERT_EQ(OB_SUCCESS, output.init(output_param, &ctx.allocator_));
    ObTablesHandleArray inputs;
    ASSERT_EQ(OB_SUCCESS, inputs.add_sstable(committed, ObStorageMetaHandle()));
    ASSERT_EQ(OB_SUCCESS, inputs.add_sstable(second_committed, ObStorageMetaHandle()));
    ObUpdateTableStoreParam update(ctx.local_.get_obj()->get_snapshot_version(),
        ObVersionRange::MIN_VERSION, ctx.schema_, ctx.ls_.get_ls()->get_rebuild_seq(),
        &output, false, true);
    ASSERT_EQ(OB_SUCCESS, update.init_with_compaction_info(ObCompactionTableStoreParam(
        compaction::INC_MAJOR_MERGE, SCN::min_scn(), false, false), nullptr));
    update.input_tables_handle_ = &inputs;
    update.ddl_info_.data_format_version_ = DATA_VERSION_4_6_3_0;
    ObArray<ObSSMetaCtrlKeyInfo> keys;
    ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->update_tablet_table_store(
        ctx.ls_id_, ctx.tablet_id_, ctx.reorganization_scn_,
        ObMetaUpdateReason::TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE, update, keys, true));
    SCN output_meta_scn;
    ASSERT_EQ(OB_SUCCESS, latest(ctx, &output_meta_scn));
    ASSERT_TRUE(ctx.shared_.get_obj()->is_async_index());
    ASSERT_EQ(OB_SUCCESS, remove_history_before(ctx, output_meta_scn));
    ASSERT_EQ(OB_OBJECT_NOT_EXIST, check_history_removed(ctx, output_meta_scn));
    // The last input's END is above the data checkpoint and its key differs from the output.
    const SCN first_end_scn = ctx.end_scn_;
    ctx.end_scn_ = SCN::plus(second_data_scn, 1000);
    ObDDLIncMajorCommitReplayExecutor last_executor;
    ASSERT_EQ(OB_SUCCESS, init_executor(ctx, second, tx_id + 100, 1, last_executor,
        true));
    bool need_publish = false;
    ASSERT_EQ(OB_SUCCESS, last_executor.deserialize_and_update_ss_inc_major(
        ctx.local_, need_publish));
    ASSERT_TRUE(need_publish);
    SCN unchanged;
    ASSERT_EQ(OB_SUCCESS, latest(ctx, &unchanged));
    ASSERT_EQ(output_meta_scn, unchanged);
    ASSERT_EQ(second_data_scn, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
    ctx.end_scn_ = first_end_scn;
    ObDDLIncMajorCommitReplayExecutor executor;
    ASSERT_EQ(OB_SUCCESS, init_executor(ctx, original, tx_id, 1, executor,
        true));
    RecEndRoleGuard role_guard;
    MTL_SET_TENANT_ROLE_CACHE(mode == 0
        ? ObTenantRole::PRIMARY_TENANT : ObTenantRole::STANDBY_TENANT);
    mock_is_sswriter = false;
    // Later Shared progress covers the old END even after its original table was removed.
    ASSERT_GT(ctx.shared_.get_obj()->get_ddl_checkpoint_scn(), ctx.end_scn_);
    ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
    ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
    int64_t count = -1;
    ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.local_, count));
    ASSERT_EQ(0, count);
    ASSERT_EQ(ctx.end_scn_, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
    // The later compaction log is independent of the DLI checkpoint.
    compaction::ObMediumCompactionInfo info(ctx.allocator_);
    ASSERT_EQ(OB_SUCCESS, info.init_data_version(DATA_VERSION_4_6_3_0));
    info.set_basic_info(ObMediumCompactionInfo::INC_MAJOR_COMPACTION,
        compaction::ObAdaptiveMergePolicy::TENANT_MAJOR, snapshot);
    info.merge_base_version_ = base_version;
    info.last_medium_snapshot_ = ctx.local_.get_obj()->get_last_major_snapshot_version();
    info.cluster_id_ = GCONF.cluster_id;
    info.tenant_id_ = MTL_ID();
    ASSERT_EQ(OB_SUCCESS, info.storage_schema_.init(
        ctx.allocator_, *ctx.schema_, false, nullptr, false, nullptr));
    ASSERT_TRUE(info.is_valid());
    compaction::ObTabletMediumClogReplayExecutor compaction_replay(info);
    const SCN compaction_scn = SCN::plus(second_commit_scn, 100);
    ASSERT_EQ(OB_SUCCESS, compaction_replay.init(compaction_scn));
    ASSERT_EQ(OB_SUCCESS, compaction_replay.execute(
        compaction_scn, ctx.ls_id_, ctx.tablet_id_));
    ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
    const ObMediumCompactionInfoList *list = nullptr;
    ASSERT_EQ(OB_SUCCESS, ctx.local_.get_obj()->read_medium_info_list(ctx.allocator_, list));
    ASSERT_NE(nullptr, list);
    ObMediumCompactionInfo::ObCompactionType type = ObMediumCompactionInfo::COMPACTION_TYPE_MAX;
    int64_t schedule_scn = 0;
    int64_t merge_base = 0;
    compaction::ObCOMajorMergeStrategy strategy;
    compaction::ObAdaptiveMergePolicy::AdaptiveMergeReason reason;
    ASSERT_EQ(OB_SUCCESS, list->get_next_schedule_info(
        ctx.local_.get_obj()->get_last_major_snapshot_version(),
        ctx.local_.get_obj()->get_last_inc_major_snapshot_version(),
        INT64_MAX, false, type, schedule_scn, merge_base, strategy, reason));
    ASSERT_EQ(ObMediumCompactionInfo::INC_MAJOR_COMPACTION, type);
    ASSERT_EQ(snapshot, schedule_scn);
    ASSERT_EQ(base_version, merge_base);
    ASSERT_EQ(OB_SUCCESS, share::SSCompactHelper::link_major(ctx.ls_id_, ctx.tablet_id_,
        compaction::INC_MAJOR_MERGE, schedule_scn, merge_base, false, false));
    ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
    ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.local_, count));
    ASSERT_EQ(1, count);
    ASSERT_EQ(snapshot, ctx.local_.get_obj()->get_last_inc_major_snapshot_version());
    ASSERT_EQ(ctx.end_scn_, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
    ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
    role_guard.restore();
    ObSqlString drop;
    ASSERT_EQ(OB_SUCCESS, drop.assign_fmt("drop table %s", names[mode]));
    ASSERT_EQ(OB_SUCCESS, exe_sql(drop.ptr()));
  }
}

TEST_F(TestDliEndRecCheckpoint, missing_result_retries_and_role_change_can_resume_publication)
{
  // Unpublished Shared state cannot advance Local; promotion allows publication to resume.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_missing", ctx));
  const SCN before = ctx.local_.get_obj()->get_ddl_checkpoint_scn();
  ObSSTable table;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77005, 5, table));
  ObDDLIncMajorCommitReplayExecutor executor;
  ASSERT_EQ(OB_SUCCESS, init_executor(ctx, table, 77005, 5, executor, true));
  RecEndRoleGuard role_guard;
  MTL_SET_TENANT_ROLE_CACHE(share::ObTenantRole::STANDBY_TENANT);
  mock_is_sswriter = false;
  ASSERT_EQ(OB_NOT_MASTER, executor.do_replay_(ctx.local_));
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
  ASSERT_EQ(before, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
  MTL_SET_TENANT_ROLE_CACHE(share::ObTenantRole::PRIMARY_TENANT);
  mock_is_sswriter = true;
  ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
  ASSERT_GE(ctx.local_.get_obj()->get_ddl_checkpoint_scn(), ctx.end_scn_);
  ASSERT_EQ(OB_SUCCESS, latest(ctx));
  ASSERT_EQ(ctx.end_scn_, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  role_guard.restore();
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_missing"));
}

TEST_F(TestDliEndRecCheckpoint, shared_backfill_keeps_publication_checkpoint)
{
  // Transaction backfill must not depend on END metadata or advance the publication checkpoint.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  const char *names[2][5] = {
      {"rec_end_commit_absent", "rec_end_commit_start", "rec_end_commit_exact",
          "rec_end_commit_invalid", "rec_end_commit_legacy"},
      {"rec_end_abort_absent", "rec_end_abort_start", "rec_end_abort_exact",
          "rec_end_abort_invalid", "rec_end_abort_legacy"}};
  const int32_t states[] = {ObTxData::COMMIT, ObTxData::ABORT};
  for (int64_t i = 0; i < 2; ++i) {
    for (int64_t mode = 0; mode < 5; ++mode) {
      RecEndContext ctx;
      ASSERT_EQ(OB_SUCCESS, prepare(names[i][mode], ctx));
      const int64_t tx_id = 77100 + i * 5 + mode;
      ObSSTable table;
      ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
          ctx.rec_scn_, tx_id, 1, table));
      const bool need_check_ddl_checkpoint = mode != 4;
      ASSERT_EQ(OB_SUCCESS, publish(ctx, table, ctx.rec_scn_, false, need_check_ddl_checkpoint));
      SCN before;
      ASSERT_EQ(OB_SUCCESS, latest(ctx, &before));
      ASSERT_EQ(DATA_VERSION_4_6_3_0,
          ctx.shared_.get_obj()->get_tablet_meta().ddl_data_format_version_);
      const SCN terminal_scn = SCN::plus(ctx.end_scn_, 200);
      ASSERT_EQ(OB_SUCCESS, seed_transaction(ctx, tx_id, states[i], terminal_scn));
      if (mode > 0 && mode < 4) {
        // Even invalid END metadata is irrelevant to transaction backfill.
        const SCN recorded_end = mode == 1 ? SCN::min_scn()
            : mode == 2 ? ctx.end_scn_ : SCN::max_scn();
        ASSERT_EQ(OB_SUCCESS, record_end_metadata(ctx, tx_id, recorded_end));
      }
      ObDirectLoadSSUpdateSharedIncMajorTask task(*ctx.ls_.get_ls(), *ctx.shared_.get_obj(),
          ctx.ls_id_, ctx.tablet_id_);
      const int update_ret = task.process();
      ASSERT_EQ(OB_SUCCESS, update_ret);
      SCN after;
      ASSERT_EQ(OB_SUCCESS, latest(ctx, &after));
      const SCN expected_checkpoint = ctx.rec_scn_;
      ASSERT_EQ(expected_checkpoint, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
      int64_t count = -1;
      ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.shared_, count));
      ASSERT_EQ(i == 0 ? 1 : 0, count);
      ASSERT_GT(after, before);
      if (count > 0) {
        ObTabletMemberWrapper<ObTabletTableStore> store;
        ASSERT_EQ(OB_SUCCESS, ctx.shared_.get_obj()->fetch_table_store_with_prewarm(store));
        ASSERT_EQ(terminal_scn.get_val_for_tx(),
            store.get_member()->get_inc_major_sstables().at(0)->get_upper_trans_version());
      }
      ObSqlString drop;
      ASSERT_EQ(OB_SUCCESS, drop.assign_fmt("drop table %s", names[i][mode]));
      ASSERT_EQ(OB_SUCCESS, exe_sql(drop.ptr()));
    }
  }
}

TEST_F(TestDliEndRecCheckpoint, shared_backfill_keeps_running_transaction_unmodified)
{
  // A recorded DLI END does not permit backfill while the transaction is still RUNNING.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_running", ctx));
  // Existing reserved-ID handling supplies RUNNING without an invalid undecided TX_DATA row.
  const int64_t tx_id = ObTransID::OB_MIN_TRANS_ID - 1;
  ObSSTable table;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, tx_id, 1, table));
  ASSERT_EQ(OB_SUCCESS, publish(ctx, table, ctx.rec_scn_, false, false));
  SCN before;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &before));
  ASSERT_EQ(OB_SUCCESS, record_end_metadata(ctx, tx_id, ctx.end_scn_));
  ObDirectLoadSSUpdateSharedIncMajorTask task(*ctx.ls_.get_ls(), *ctx.shared_.get_obj(),
      ctx.ls_id_, ctx.tablet_id_);
  ASSERT_EQ(OB_SUCCESS, task.process());
  SCN after;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &after));
  ASSERT_EQ(before, after);
  ASSERT_EQ(ctx.rec_scn_, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  ObTabletMemberWrapper<ObTabletTableStore> store;
  ASSERT_EQ(OB_SUCCESS, ctx.shared_.get_obj()->fetch_table_store_with_prewarm(store));
  ASSERT_EQ(1, store.get_member()->get_inc_major_sstables().count());
  ASSERT_EQ(INT64_MAX,
      store.get_member()->get_inc_major_sstables().at(0)->get_upper_trans_version());
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_running"));
}

TEST_F(TestDliEndRecCheckpoint, transfer_backfill_keeps_fill_boundary_and_publication_checkpoint)
{
  // Transfer updates transaction metadata while retaining the original publication checkpoint.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  const char *names[] = {
      "rec_end_tf_absent", "rec_end_tf_start", "rec_end_tf_exact", "rec_end_tf_invalid"};
  for (int64_t mode = 0; mode < 4; ++mode) {
    RecEndContext ctx;
    ASSERT_EQ(OB_SUCCESS, prepare(names[mode], ctx));
    const int64_t tx_id = 77110 + mode;
    ObSSTable table;
    ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
        ctx.rec_scn_, tx_id, 1, table));
    ASSERT_EQ(OB_SUCCESS, publish(ctx, table, ctx.rec_scn_, false, false));
    SCN before;
    ASSERT_EQ(OB_SUCCESS, latest(ctx, &before));
    const SCN terminal_scn = SCN::plus(ctx.end_scn_, 200);
    const SCN backfill_scn = SCN::plus(terminal_scn, 1000000);
    ASSERT_EQ(OB_SUCCESS, seed_transaction(ctx, tx_id, ObTxData::COMMIT, terminal_scn));
    if (mode > 0) {
      // END metadata does not participate in transfer transaction backfill.
      const SCN recorded_end = mode == 1 ? SCN::min_scn()
          : mode == 2 ? ctx.end_scn_ : SCN::max_scn();
      ASSERT_EQ(OB_SUCCESS, record_end_metadata(ctx, tx_id, recorded_end));
    }
    ObTabletMemberWrapper<ObTabletTableStore> task_store;
    ObSSTabletIncMajorBackfillTXTask task;
    task.is_inited_ = true;
    task.ls_id_ = ctx.ls_id_;
    task.tablet_info_.tablet_id_ = ctx.tablet_id_;
    task.tablet_info_.reorganization_scn_ = ctx.reorganization_scn_;
    task.tablet_info_.backfill_scn_ = backfill_scn;
    SCN shared_meta_scn;
    ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->get_tablet(ctx.ls_id_, ctx.tablet_id_,
        ctx.reorganization_scn_, task.tablet_handle_allocator_,
        task.tablet_handle_, shared_meta_scn));
    ASSERT_EQ(OB_SUCCESS, task.tablet_handle_.get_obj()->fetch_table_store_with_prewarm(
        task_store));
    // Borrow the actual Shared result while its tablet and store wrapper stay alive.
    ASSERT_EQ(OB_SUCCESS, task.table_handle_.set_sstable_with_tablet(
        task_store.get_member()->get_inc_major_sstables().at(0)));
    ASSERT_EQ(OB_SUCCESS, task.do_backfill_());
    SCN after;
    ASSERT_EQ(OB_SUCCESS, latest(ctx, &after));
    const SCN expected_checkpoint = ctx.rec_scn_;
    ASSERT_EQ(expected_checkpoint, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
    ObTabletMemberWrapper<ObTabletTableStore> store;
    ASSERT_EQ(OB_SUCCESS, ctx.shared_.get_obj()->fetch_table_store_with_prewarm(store));
    ASSERT_EQ(1, store.get_member()->get_inc_major_sstables().count());
    const ObSSTable *result = store.get_member()->get_inc_major_sstables().at(0);
    ASSERT_GT(after, before);
    ASSERT_EQ(terminal_scn.get_val_for_tx(), result->get_upper_trans_version());
    ASSERT_EQ(backfill_scn, result->get_filled_tx_scn());
    ObSqlString drop;
    ASSERT_EQ(OB_SUCCESS, drop.assign_fmt("drop table %s", names[mode]));
    ASSERT_EQ(OB_SUCCESS, exe_sql(drop.ptr()));
  }
}

TEST_F(TestDliEndRecCheckpoint, first_shared_creation_requires_writer_and_completes_add)
{
  // A qualified writer creates the missing Shared tablet and publishes in one call.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_first", ctx, false));
  ObSSTable table;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77020, 1, table));
  ObDDLIncMajorCommitReplayExecutor executor;
  ASSERT_EQ(OB_SUCCESS, init_executor(ctx, table, 77020, 1, executor, true));
  RecEndRoleGuard role_guard;
  bool publish_needed = false;
  mock_is_sswriter = false;
  ASSERT_EQ(OB_NOT_MASTER, executor.deserialize_and_update_ss_inc_major(
      ctx.local_, publish_needed));
  mock_is_sswriter = true;
  ASSERT_EQ(OB_SUCCESS, executor.deserialize_and_update_ss_inc_major(ctx.local_, publish_needed));
  ASSERT_EQ(OB_SUCCESS, latest(ctx));
  ASSERT_EQ(ctx.end_scn_, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  int64_t count = -1;
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.shared_, count));
  ASSERT_EQ(1, count);
  role_guard.restore();
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_first"));
}

TEST_F(TestDliEndRecCheckpoint, empty_batch_finishes_without_shared_publication)
{
  // Repeated empty END needs neither Shared metadata nor a Local checkpoint update.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_empty", ctx, false, "a int"));
  const SCN local_before = ctx.local_.get_obj()->get_ddl_checkpoint_scn();
  ObSSTable empty;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      SCN::min_scn(), 77021, 1, empty, ObITable::INC_MAJOR_SSTABLE, INT64_MAX, true));
  ObDDLIncMajorCommitReplayExecutor executor;
  ASSERT_EQ(OB_SUCCESS, init_executor(ctx, empty, 77021, 1, executor, true));
  RecEndRoleGuard role_guard;
  mock_is_sswriter = false;
  for (int64_t attempt = 0; attempt < 2; ++attempt) {
    ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
    ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(
        ctx.tablet_id_, ctx.local_, ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT,
        ObMDSGetTabletMode::READ_ALL_COMMITED));
    ASSERT_EQ(local_before, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
    int64_t count = -1;
    ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.local_, count));
    ASSERT_EQ(0, count);
    ASSERT_EQ(OB_TABLET_NOT_EXIST, latest(ctx, nullptr));
  }
  role_guard.restore();
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_empty"));
}

TEST_F(TestDliEndRecCheckpoint, finite_empty_batch_finishes_after_shared_progress)
{
  // Finite empty END leaves Local progress unchanged even after Shared advances.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_finite_empty", ctx));
  ASSERT_FALSE(ctx.local_.get_obj()->is_async_index_refresh_target());
  ObSSTable empty;
  ObSSTable later;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77022, 1, empty, ObITable::INC_MAJOR_SSTABLE, INT64_MAX, true));
  ASSERT_TRUE(empty.is_empty());
  ASSERT_TRUE(empty.get_end_scn().is_valid_and_not_min());
  const SCN later_scn = SCN::plus(ctx.end_scn_, 2000);
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      later_scn, 77022, 2, later));
  ASSERT_EQ(OB_SUCCESS, publish(ctx, later, later_scn, false, false));
  SCN published_version;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &published_version));
  ASSERT_GT(ctx.shared_.get_obj()->get_ddl_checkpoint_scn(), ctx.end_scn_);
  ASSERT_LT(ctx.local_.get_obj()->get_ddl_checkpoint_scn(), ctx.end_scn_);
  ObDDLIncLogBasic basic;
  ASSERT_EQ(OB_SUCCESS, basic.init(ctx.tablet_id_, ObTabletID(), ObTabletID(),
      ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR, ObTransID(77022),
      ObTxSEQ::from_seq(1), ctx.local_.get_obj()->get_snapshot_version(),
      DATA_VERSION_4_6_3_0));
  ObDDLIncCommitLog log;
  ASSERT_EQ(OB_SUCCESS, log.init(basic, false, SCN::scn_dec(ctx.rec_scn_)));
  ASSERT_EQ(OB_SUCCESS, log.set_ss_inc_major(&empty, nullptr, nullptr, true));
  ObDDLRedoLogReplayer replayer;
  ASSERT_EQ(OB_SUCCESS, replayer.init(ctx.ls_.get_ls()));
  RecEndRoleGuard role_guard;
  mock_is_sswriter = false;
  const SCN local_before = ctx.local_.get_obj()->get_ddl_checkpoint_scn();
  for (int64_t attempt = 0; attempt < 2; ++attempt) {
    ASSERT_EQ(OB_SUCCESS, replayer.replay_inc_commit(log, ctx.end_scn_));
    ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
    ASSERT_EQ(local_before, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
    int64_t count = -1;
    ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.local_, count));
    ASSERT_EQ(0, count);
    SCN after_version;
    ASSERT_EQ(OB_SUCCESS, latest(ctx, &after_version));
    ASSERT_EQ(published_version, after_version);
    ASSERT_EQ(later_scn, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
    ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.shared_, count));
    ASSERT_EQ(1, count);
  }
  role_guard.restore();
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_finite_empty"));
}

void TestDliEndRecCheckpoint::check_lob_end_retry(const bool empty_lob_piece)
{
  // Retry after the main result completes and the nonempty LOB dependency is supplied.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext contexts[3];
  const char *name = empty_lob_piece ? "rec_end_empty_piece_retry" : "rec_end_lob_retry";
  ASSERT_EQ(OB_SUCCESS, prepare(name, contexts[0], true,
      "a bigint primary key, b longtext"));
  RecEndContext &main = contexts[0];
  ObTabletBindingMdsUserData binding;
  ASSERT_EQ(OB_SUCCESS, main.local_.get_obj()->ObITabletMdsInterface::get_ddl_data(
      SCN::max_scn(), binding));
  const ObTabletID targets[] = {
      main.tablet_id_, binding.lob_meta_tablet_id_, binding.lob_piece_tablet_id_};
  ObSSTable tables[3];
  SCN local_before[3];
  for (int64_t i = 0; i < 3; ++i) {
    RecEndContext &ctx = contexts[i];
    ASSERT_TRUE(targets[i].is_valid());
    if (i > 0) {
      ctx.tablet_id_ = targets[i];
      ctx.ls_id_ = main.ls_id_;
      ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(
          ctx.ls_id_, ctx.ls_, ObLSGetMod::STORAGE_MOD));
      ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
      ctx.reorganization_scn_ = ctx.local_.get_obj()->get_reorganization_scn();
      ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->create_tablet(
          ctx.ls_id_, ctx.tablet_id_, ctx.reorganization_scn_, true));
      ASSERT_EQ(OB_SUCCESS, ctx.local_.get_obj()->load_storage_schema(
          ctx.allocator_, ctx.schema_));
      ctx.rec_scn_ = main.rec_scn_;
      ctx.end_scn_ = main.end_scn_;
    }
    local_before[i] = ctx.local_.get_obj()->get_ddl_checkpoint_scn();
    ASSERT_LT(local_before[i], main.end_scn_);
    ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
        ctx.rec_scn_, 77023, 1, tables[i], ObITable::INC_MAJOR_SSTABLE,
        INT64_MAX, empty_lob_piece && i == 2));
    ASSERT_EQ(empty_lob_piece && i == 2, tables[i].is_empty());
    ObTabletMemberWrapper<ObTabletTableStore> store;
    ASSERT_EQ(OB_SUCCESS, ctx.local_.get_obj()->fetch_table_store_with_prewarm(store));
    const ObSSTableArray &majors = store.get_member()->get_major_sstables();
    for (int64_t j = 0; j < majors.count(); ++j) {
      ASSERT_LT(majors.at(j)->get_snapshot_version(), tables[i].get_max_merged_trans_version());
    }
  }
  SCN piece_meta_before;
  if (empty_lob_piece) {
    ASSERT_EQ(OB_SUCCESS, latest(contexts[2], &piece_meta_before));
  }
  ObDDLIncLogBasic basic;
  ASSERT_EQ(OB_SUCCESS, basic.init(targets[0], targets[1], targets[2],
      ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR, ObTransID(77023),
      ObTxSEQ::from_seq(1), main.local_.get_obj()->get_snapshot_version(),
      DATA_VERSION_4_6_3_0));
  ObDDLIncCommitLog log;
  ASSERT_EQ(OB_SUCCESS, log.init(basic, false, SCN::scn_dec(main.rec_scn_)));
  ASSERT_EQ(OB_SUCCESS, log.set_ss_inc_major(&tables[0], &tables[1], &tables[2], true));
  ObDDLRedoLogReplayer replayer;
  ASSERT_EQ(OB_SUCCESS, replayer.init(main.ls_.get_ls()));
  ASSERT_EQ(OB_SUCCESS, publish(main, tables[0], main.rec_scn_, false, false));
  RecEndRoleGuard role_guard;
  MTL_SET_TENANT_ROLE_CACHE(share::ObTenantRole::PRIMARY_TENANT);
  mock_is_sswriter = false;
  ASSERT_EQ(OB_EAGAIN, replayer.replay_inc_commit(log, main.end_scn_));
  for (int64_t i = 0; i < 3; ++i) {
    RecEndContext &ctx = contexts[i];
    ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
    ASSERT_EQ(i == 0 ? main.end_scn_ : local_before[i],
        ctx.local_.get_obj()->get_ddl_checkpoint_scn());
    int64_t count = -1;
    ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.local_, count));
    ASSERT_EQ(i == 0 ? 1 : 0, count);
  }
  // Publish the missing LOB dependencies, then retry as the same non-writer follower.
  mock_is_sswriter = true;
  ASSERT_EQ(OB_SUCCESS, publish(contexts[1], tables[1], main.rec_scn_,
      false, false));
  if (!empty_lob_piece) {
    ASSERT_EQ(OB_SUCCESS, publish(contexts[2], tables[2], main.rec_scn_,
        false, false));
  }
  mock_is_sswriter = false;
  for (int64_t attempt = 0; attempt < 2; ++attempt) {
    ASSERT_EQ(OB_SUCCESS, replayer.replay_inc_commit(log, main.end_scn_));
    for (int64_t i = 0; i < 3; ++i) {
      RecEndContext &ctx = contexts[i];
      ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
      const bool empty_result = empty_lob_piece && i == 2;
      ASSERT_EQ(empty_result ? local_before[i] : main.end_scn_,
          ctx.local_.get_obj()->get_ddl_checkpoint_scn());
      ObTabletMemberWrapper<ObTabletTableStore> store;
      ASSERT_EQ(OB_SUCCESS, ctx.local_.get_obj()->fetch_table_store_with_prewarm(store));
      const ObSSTableArray &results = store.get_member()->get_inc_major_sstables();
      if (empty_result) {
        ASSERT_EQ(0, results.count());
        SCN piece_meta_after;
        ASSERT_EQ(OB_SUCCESS, latest(ctx, &piece_meta_after));
        ASSERT_EQ(piece_meta_before, piece_meta_after);
        int64_t count = -1;
        ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.shared_, count));
        ASSERT_EQ(0, count);
      } else {
        ASSERT_EQ(1, results.count());
        ASSERT_EQ(tables[i].get_key(), results.at(0)->get_key());
        ObTransID trans_id;
        ObTxSEQ seq_no;
        ASSERT_EQ(OB_SUCCESS, compaction::ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(
            results.at(0), trans_id, seq_no));
        ASSERT_EQ(ObTransID(77023), trans_id);
        ASSERT_EQ(ObTxSEQ::from_seq(1), seq_no);
      }
    }
  }
  role_guard.restore();
  ObSqlString drop;
  ASSERT_EQ(OB_SUCCESS, drop.assign_fmt("drop table %s", name));
  ASSERT_EQ(OB_SUCCESS, exe_sql(drop.ptr()));
}

TEST_F(TestDliEndRecCheckpoint, follower_retries_lob_after_main_completion)
{
  // All three nonempty results complete independently after a missing LOB is published.
  check_lob_end_retry(false);
}

TEST_F(TestDliEndRecCheckpoint, follower_retries_lob_with_empty_piece)
{
  // An empty LOB piece keeps its checkpoint while nonempty main/LOB results complete.
  check_lob_end_retry(true);
}

TEST_F(TestDliEndRecCheckpoint, compaction_does_not_wait_for_checkpoint_backfill)
{
  // A committed compaction can replace raw MAX inputs without waiting for END backfill.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_compact", ctx));
  ObSSTable table;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77022, 1, table));
  ASSERT_EQ(OB_SUCCESS, publish(ctx, table, ctx.rec_scn_, false, false));
  ObSSTable *committed = nullptr;
  ASSERT_EQ(OB_SUCCESS, table.deep_copy(ctx.allocator_, committed));
  const SCN terminal_scn = SCN::plus(ctx.end_scn_, 100);
  ASSERT_EQ(OB_SUCCESS, seed_transaction(ctx, 77022, ObTxData::COMMIT, terminal_scn));
  ASSERT_EQ(OB_SUCCESS, committed->set_upper_trans_version(
      ctx.allocator_, terminal_scn.get_val_for_tx()));
  ObTablesHandleArray old_inputs;
  ASSERT_EQ(OB_SUCCESS, old_inputs.add_sstable(&table, ObStorageMetaHandle()));
  ObUpdateTableStoreParam param;
  ASSERT_EQ(OB_SUCCESS, ObSSDDLUtil::build_update_table_store_param_(
      *ctx.ls_.get_ls(), *committed, *ctx.schema_, DATA_VERSION_4_6_3_0, param));
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::COMPACTION;
  param.input_tables_handle_ = &old_inputs;
  ObArray<ObSSMetaCtrlKeyInfo> keys;
  ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->update_tablet_table_store(
      ctx.ls_id_, ctx.tablet_id_, ctx.reorganization_scn_,
      ObMetaUpdateReason::TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE, param, keys, true));
  ASSERT_EQ(OB_NO_NEED_MERGE, MTL(ObSSMetaService *)->update_tablet_table_store(
      ctx.ls_id_, ctx.tablet_id_, ctx.reorganization_scn_,
      ObMetaUpdateReason::TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE, param, keys, true));
  ObTablesHandleArray inputs;
  ASSERT_EQ(OB_SUCCESS, inputs.add_sstable(committed, ObStorageMetaHandle()));
  param.input_tables_handle_ = &inputs;
  ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->update_tablet_table_store(
      ctx.ls_id_, ctx.tablet_id_, ctx.reorganization_scn_,
      ObMetaUpdateReason::TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE, param, keys, true));
  ASSERT_EQ(OB_SUCCESS, latest(ctx));
  ASSERT_EQ(ctx.rec_scn_, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  SCN before;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &before));
  ObDDLIncMajorCommitReplayExecutor executor;
  ASSERT_EQ(OB_SUCCESS, init_executor(ctx, table, 77022, 1, executor, true));
  bool need_publish = false;
  ASSERT_EQ(OB_SUCCESS, executor.deserialize_and_update_ss_inc_major(
      ctx.local_, need_publish));
  ASSERT_TRUE(need_publish);
  SCN after;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &after));
  ASSERT_EQ(before, after);
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_compact"));
}

TEST_F(TestDliEndRecCheckpoint, final_publication_uses_latest_batch_scn_without_changing_table_key)
{
  // Late REDO in an active KV advances rec_scn and publication without changing the TableKey.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_publish_boundary", ctx, true, "a int"));
  const ObTransID trans_id(77312);
  const ObTxSEQ seq_no = ObTxSEQ::from_seq(1);
  SCN active_scn;
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_max_decided_scn(active_scn));
  ASSERT_TRUE(active_scn.is_valid_and_not_min());
  const SCN frozen_scn = SCN::minus(active_scn, 100);
  ctx.rec_scn_ = SCN::minus(frozen_scn, 100);
  ctx.end_scn_ = SCN::plus(active_scn, 1000);
  const SCN start_scn = SCN::scn_dec(ctx.rec_scn_);
  ObDDLKvMgrHandle kv_mgr;
  ObDDLKVHandle kv;
  ASSERT_EQ(OB_SUCCESS, ctx.local_.get_obj()->get_ddl_kv_mgr(kv_mgr, true));
  ASSERT_EQ(OB_SUCCESS, kv_mgr.get_obj()->get_or_create_inc_major_ddl_kv(
      frozen_scn, start_scn, ctx.local_.get_obj()->get_snapshot_version(),
      DATA_VERSION_4_6_3_0, trans_id, seq_no, ObITable::INC_MAJOR_DDL_MEM_SSTABLE, kv));
  ASSERT_TRUE(kv.is_valid());
  ASSERT_EQ(OB_SUCCESS, kv.get_obj()->freeze(frozen_scn));
  ASSERT_TRUE(kv.get_obj()->is_freezed());
  ObDDLKVHandle active_kv;
  ASSERT_EQ(OB_SUCCESS, kv_mgr.get_obj()->get_or_create_inc_major_ddl_kv(
      active_scn, start_scn, ctx.local_.get_obj()->get_snapshot_version(),
      DATA_VERSION_4_6_3_0, trans_id, seq_no, ObITable::INC_MAJOR_DDL_MEM_SSTABLE, active_kv));
  ASSERT_TRUE(active_kv.is_valid());
  ASSERT_FALSE(active_kv.get_obj()->is_freezed());
  // Model the SCN recorded by the last successful index REDO callback.
  active_kv.get_obj()->min_scn_ = active_scn;
  active_kv.get_obj()->max_scn_ = active_scn;

  ObDDLTabletContext tablet_ctx;
  tablet_ctx.ls_id_ = ctx.ls_id_;
  tablet_ctx.tablet_id_ = ctx.tablet_id_;
  tablet_ctx.tablet_param_.storage_schema_ = ctx.schema_;
  ASSERT_EQ(OB_SUCCESS, tablet_ctx.merge_ctx_.init(DIRECT_LOAD_INCREMENTAL_MAJOR));
  ObDDLTaskParam task_param;
  task_param.tenant_data_version_ = DATA_VERSION_4_6_3_0;
  task_param.snapshot_version_ = ctx.local_.get_obj()->get_snapshot_version();
  task_param.schema_version_ = ctx.schema_->get_schema_version();
  task_param.ddl_task_id_ = 1;
  task_param.target_table_id_ = 1;
  ObDDLTabletMergeDagParamV2 merge_param;
  ASSERT_EQ(OB_SUCCESS, merge_param.init(true, ObDirectLoadTabletTarget::DATA, false,
      start_scn, DIRECT_LOAD_INCREMENTAL_MAJOR, task_param, &tablet_ctx, trans_id, seq_no, 0));
  merge_param.rec_scn_ = frozen_scn;
  ObSSIncMajorDDLMergeHelper helper;
  ObArray<ObDDLKVHandle> frozen_kvs;
  ObTableStoreIterator dumps;
  SCN min_scn;
  SCN max_scn;
  ASSERT_EQ(OB_SUCCESS, helper.get_ddl_kvs_and_ddl_dump_sstables(
      ctx.local_, merge_param, true, frozen_kvs, dumps));
  ASSERT_EQ(OB_SUCCESS, helper.get_min_max_scn(frozen_kvs, dumps, min_scn, max_scn));
  ASSERT_EQ(frozen_scn, max_scn);

  ObSSTable table;
  ASSERT_EQ(OB_SUCCESS, build_rec_end_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77312, 1, table, ObITable::INC_MAJOR_SSTABLE, INT64_MAX, false));
  const ObITable::TableKey table_key = table.get_key();
  ASSERT_GT(max_scn, table.get_end_scn());
  ASSERT_EQ(OB_SUCCESS, latest(ctx, nullptr));
  ASSERT_LT(ctx.shared_.get_obj()->get_ddl_checkpoint_scn(), table.get_end_scn());
  ASSERT_LT(ctx.local_.get_obj()->get_ddl_checkpoint_scn(), table.get_end_scn());
  ASSERT_EQ(OB_SUCCESS, helper.update_tablet_table_store_for_inc_major(merge_param, &table));
  ASSERT_EQ(active_scn, merge_param.rec_scn_);
  ASSERT_TRUE(table_key == table.get_key());
  ASSERT_EQ(OB_SUCCESS, latest(ctx, nullptr));
  ASSERT_EQ(active_scn, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(
      ctx.tablet_id_, ctx.local_, ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT,
      ObMDSGetTabletMode::READ_ALL_COMMITED));
  ASSERT_EQ(active_scn, ctx.local_.get_obj()->get_ddl_checkpoint_scn());

  ASSERT_EQ(OB_SUCCESS, helper.release_ddl_kv_for_major(merge_param, kv_mgr));

  // END replay preserves Shared publication progress and completes Local progress.
  ObDDLIncMajorCommitReplayExecutor executor;
  ASSERT_EQ(OB_SUCCESS, init_executor(ctx, table, 77312, 1, executor, true));
  ASSERT_EQ(OB_SUCCESS, executor.do_replay_(ctx.local_));
  ASSERT_EQ(active_scn, merge_param.rec_scn_);
  ASSERT_TRUE(table_key == table.get_key());
  ASSERT_EQ(OB_SUCCESS, latest(ctx, nullptr));
  ASSERT_EQ(active_scn, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(
      ctx.tablet_id_, ctx.local_, ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT,
      ObMDSGetTabletMode::READ_ALL_COMMITED));
  ASSERT_EQ(ctx.end_scn_, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
  // A later batch with no dumps must not wait for the previous END to reach Shared.
  ObDDLIncLogBasic next_batch;
  ASSERT_EQ(OB_SUCCESS, next_batch.init(ctx.tablet_id_, ObTabletID(), ObTabletID(),
      DIRECT_LOAD_INCREMENTAL_MAJOR, trans_id, ObTxSEQ::from_seq(2),
      ctx.local_.get_obj()->get_snapshot_version(), DATA_VERSION_4_6_3_0));
  bool pending = true;
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_inc_sstable_uploader().check_inc_major_ddl_checkpoint_(
      next_batch, pending));
  ASSERT_FALSE(pending);
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_publish_boundary"));
}

TEST_F(TestDliEndRecCheckpoint, primary_end_checkpoint_flag_follows_async_index_activation)
{
  // Real 4.6.3.0 ENDs retain their opt-in flag after the tenant configuration changes.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  omt::ObTenantConfigGuard config(TENANT_CONF(MTL_ID()));
  ASSERT_TRUE(config.is_valid());
  const bool saved_enabled = config->_enable_async_index;
  ObClusterVersion &version = ObClusterVersion::get_instance();
  const uint64_t saved_version = ATOMIC_LOAD(&version.data_version_);
  const uint64_t saved_mock = ATOMIC_LOAD(&ODV_MGR.mock_data_version_);
  DEFER({
    config->_enable_async_index = saved_enabled;
    version.update_data_version(saved_version);
    ODV_MGR.set_mock_data_version(saved_mock);
  });
  version.update_data_version(0);
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_0);
  for (int64_t mode = 0; mode < 2; ++mode) {
    const bool enabled = mode != 0;
    config->_enable_async_index = enabled;
    ASSERT_EQ(enabled, ObAsyncIndexRefreshUtil::is_async_index_enabled(MTL_ID()));
    const char *name = enabled ? "rec_end_gate_on" : "rec_end_gate_off";
    ObSqlString sql;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table %s (a bigint primary key)", name));
    ASSERT_EQ(OB_SUCCESS, exe_sql(sql.ptr()));
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(
        "insert /*+ direct(true, 0, 'inc_replace') enable_parallel_dml parallel(2) "
        "query_timeout(120000000) */ into %s "
        "select row_number() over() from table(generator(4))", name));
    ASSERT_EQ(OB_SUCCESS, exe_sql(sql.ptr()));
    ObTabletID tablet_id;
    ObLSID ls_id;
    ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id(name, tablet_id, ls_id));
    ObLSHandle ls;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls, ObLSGetMod::STORAGE_MOD));
    ObTabletHandle local;
    ASSERT_EQ(OB_SUCCESS, ls.get_ls()->get_tablet(
        tablet_id, local, ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT,
        ObMDSGetTabletMode::READ_ALL_COMMITED));
    ObTabletMemberWrapper<ObTabletTableStore> store;
    ASSERT_EQ(OB_SUCCESS, local.get_obj()->fetch_table_store_with_prewarm(store));
    ASSERT_GT(store.get_member()->get_inc_major_sstables().count(), 0);
    const ObSSTable *result = store.get_member()->get_inc_major_sstables().at(0);
    ObTransID trans_id;
    ObTxSEQ seq_no;
    ASSERT_EQ(OB_SUCCESS, compaction::ObIncMajorTxHelper::get_trans_id_and_seq_no(
        *result, trans_id, seq_no));
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "RecEndGate"));
    ObTabletDDLCompleteMdsUserData completed;
    ASSERT_EQ(OB_SUCCESS, local.get_obj()->get_inc_major_direct_load_info(
        SCN::max_scn(), allocator, ObTabletDDLCompleteMdsUserDataKey(trans_id), completed));
    ASSERT_TRUE(completed.inc_major_commit_scn_.is_valid_and_not_min());

    // Read the committed transaction log, not the current parameter or an archive filename.
    config->_enable_async_index = !enabled;
    palf::PalfBufferIterator iterator;
    ASSERT_EQ(OB_SUCCESS, ls.get_ls()->get_log_handler()->seek(
        completed.inc_major_commit_scn_, iterator));
    ASSERT_EQ(OB_SUCCESS, iterator.next());
    const char *buf = nullptr;
    int64_t length = 0;
    SCN scn;
    palf::LSN lsn;
    bool raw_write = false;
    ASSERT_EQ(OB_SUCCESS, iterator.get_entry(buf, length, scn, lsn, raw_write));
    ASSERT_EQ(completed.inc_major_commit_scn_, scn);
    transaction::ObTxLogBlock block;
    ASSERT_EQ(OB_SUCCESS, block.init_for_replay(buf, length));
    transaction::ObTxLogHeader header(transaction::ObTxLogType::UNKNOWN);
    bool found_end = false;
    while (!found_end) {
      ASSERT_EQ(OB_SUCCESS, block.get_next_log(header, nullptr, nullptr));
      if (transaction::ObTxLogType::TX_DIRECT_LOAD_INC_MAJOR_LOG == header.get_tx_log_type()) {
        transaction::ObTxDirectLoadIncLog::TempRef temp;
        transaction::ObTxDirectLoadIncLog::ConstructArg arg(temp);
        transaction::ObTxDirectLoadIncMajorLog tx_log(arg);
        ASSERT_EQ(OB_SUCCESS, block.deserialize_log_body(tx_log));
        if (transaction::ObTxDirectLoadIncLog::DirectLoadIncLogType::DLI_END
            == tx_log.get_ddl_log_type()) {
          ObDDLIncCommitLog end;
          ASSERT_EQ(OB_SUCCESS, tx_log.get_dli_buf().deserialize_log_object(&end));
          ASSERT_EQ(tablet_id, end.get_log_basic().get_tablet_id());
          ASSERT_EQ(DATA_VERSION_4_6_3_0, end.get_log_basic().get_data_format_version());
          ASSERT_EQ(enabled, end.need_check_ddl_checkpoint());
          found_end = true;
        }
      }
    }
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("drop table %s", name));
    ASSERT_EQ(OB_SUCCESS, exe_sql(sql.ptr()));
  }
}

#ifdef ERRSIM
TEST_F(TestDliEndRecCheckpoint, inc_kv_pressure_schedules_on_allocation)
{
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  // Prove pressure scheduling works without a periodic MergeLoop, and exclude timer races.
  auto &timer = MTL(compaction::ObTenantTabletScheduler *)->timer_task_mgr_;
  ASSERT_EQ(OB_SUCCESS, TG_CANCEL_TASK_R(timer.merge_loop_tg_id_, timer.merge_loop_task_));
  DEFER(EXPECT_EQ(OB_SUCCESS, TG_SCHEDULE(timer.merge_loop_tg_id_,
      timer.merge_loop_task_, timer.schedule_interval_, true /*repeat*/)));
  ASSERT_EQ(OB_SUCCESS, TG_INVOKE(timer.merge_loop_tg_id_, wait_task, timer.merge_loop_task_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_kv_pressure", ctx));
  ObDDLKvMgrHandle mgr;
  ASSERT_EQ(OB_SUCCESS, ctx.local_.get_obj()->get_ddl_kv_mgr(mgr, true));
  NamedEventGuard schedule_failure;
  ASSERT_EQ(OB_SUCCESS, schedule_failure.init("EN_COMPACTION_DISABLE_INC_MAJOR_SSTABLE", OB_EAGAIN, 100));
  DEFER(mgr.get_obj()->cleanup(); exe_sql("drop table rec_end_kv_pressure"));
  SCN scn;
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_max_decided_scn(scn));
  scn = SCN::minus(scn, 1000);
  ASSERT_EQ(OB_SUCCESS, mgr.get_obj()->set_max_freeze_scn(scn));
  const int64_t threshold = ObTabletDDLKvMgr::MAX_DDL_KV_CNT_TO_DUMP;
  for (int64_t i = 0; i < threshold; ++i) {
    scn = SCN::plus(scn, 1);
    ObDDLKVHandle kv;
    ASSERT_EQ(OB_SUCCESS, mgr.get_obj()->get_or_create_inc_major_ddl_kv(
        scn, SCN::base_scn(), ctx.local_.get_obj()->get_snapshot_version(), DATA_VERSION_4_6_3_0,
        ObTransID(77401), ObTxSEQ::from_seq(i + 1), ObITable::INC_MAJOR_DDL_MEM_SSTABLE, kv));
    ASSERT_TRUE(kv.is_valid());
    EXPECT_EQ(i + 1, mgr.get_obj()->get_count());
    EXPECT_EQ(i + 1 < threshold ? 0 : 1, schedule_failure.get_trigger_count());
    // Model a completed REDO; allocation, freezing and scheduling use the real implementations.
    kv.get_obj()->min_scn_ = kv.get_obj()->max_scn_ = scn;
    ASSERT_EQ(OB_SUCCESS, mgr.get_obj()->freeze_ddl_kv(SCN::base_scn(),
        ctx.local_.get_obj()->get_snapshot_version(), DATA_VERSION_4_6_3_0, SCN::min_scn(),
        ObDDLKVType::DDL_KV_INC_MAJOR, ObTransID(77401), ObTxSEQ::from_seq(i + 1),
        ObITable::INC_MAJOR_DDL_MEM_SSTABLE));
    ASSERT_TRUE(kv.get_obj()->is_freezed());
    EXPECT_EQ(i + 1 < threshold ? 0 : 1, schedule_failure.get_trigger_count());
  }
}

TEST_F(TestDliEndRecCheckpoint, inc_kv_full_queue_dump_skips_duplicate_preparation)
{
  // Full-queue retries must schedule a real dump without repeating preparation for a live DAG.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  auto &timer = MTL(compaction::ObTenantTabletScheduler *)->timer_task_mgr_;
  ASSERT_EQ(OB_SUCCESS, TG_CANCEL_TASK_R(timer.merge_loop_tg_id_, timer.merge_loop_task_));
  DEFER(EXPECT_EQ(OB_SUCCESS, TG_SCHEDULE(timer.merge_loop_tg_id_,
      timer.merge_loop_task_, timer.schedule_interval_, true /*repeat*/)));
  ASSERT_EQ(OB_SUCCESS, TG_INVOKE(timer.merge_loop_tg_id_, wait_task, timer.merge_loop_task_));
  RecEndContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("rec_end_kv_pending", ctx));
  const ObTransID trans_id(77403);
  ObDDLKvMgrHandle mgr;
  ObDDLKVHandle first;
  ObDDLTableMergeDag dag;
  SCN end_scn;
  ASSERT_EQ(OB_SUCCESS, prepare_full_inc_kv_queue(ctx, trans_id, mgr, first, dag, end_scn));
  DEFER({
    (void)MTL(ObTenantDagScheduler *)->cancel_dag(&dag, true);
    if (wait_for_inc_kv_condition([&]() { return inc_kv_dag_finished(dag); })) {
      mgr.get_obj()->cleanup();
      EXPECT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_kv_pending"));
    } else {
      ADD_FAILURE() << "dump DAG did not stop during cleanup";
    }
  });
  first.get_obj()->inc_pending_cnt();
  bool pending = true;
  DEFER(if (pending) { first.get_obj()->dec_pending_cnt(); });
  ASSERT_EQ(OB_EAGAIN, first.get_obj()->wait_pending());
  const SCN checkpoint_before = ctx.local_.get_obj()->get_ddl_checkpoint_scn();
  ObDDLKVHandle next;
  SCN start_scn;
  ASSERT_EQ(OB_SUCCESS, start_scn.convert_for_tx(SS_DDL_START_SCN_VAL));
  const int64_t before_create = ddl_merge_dag_create_count;
  ASSERT_EQ(OB_EAGAIN, mgr.get_obj()->get_or_create_inc_major_ddl_kv(
      SCN::plus(end_scn, 1), start_scn, ctx.local_.get_obj()->get_snapshot_version(), DATA_VERSION_4_6_3_0,
      trans_id, ObTxSEQ::from_seq(1), ObITable::INC_MAJOR_DDL_MEM_SSTABLE, next));
  ASSERT_FALSE(next.is_valid());
  ASSERT_EQ(before_create + 1, ddl_merge_dag_create_count);
  // The DAG owns the tablet lock while close_ddl_kvs waits for the held pending count.
  ASSERT_TRUE(wait_for_inc_kv_condition([&]() {
    int ret = MTL(ObDDLMergeBucketLock *)->lock(ctx.tablet_id_);
    if (OB_SUCC(ret)) {
      EXPECT_EQ(OB_SUCCESS, MTL(ObDDLMergeBucketLock *)->unlock(ctx.tablet_id_));
    }
    return OB_EAGAIN == ret;
  }));
  const int64_t capacity = ObTabletDDLKvMgr::MAX_DDL_KV_CNT_IN_STORAGE;
  EXPECT_EQ(capacity, mgr.get_obj()->get_count());
  EXPECT_FALSE(first.get_obj()->is_closed());
  ASSERT_EQ(OB_SUCCESS, ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_));
  EXPECT_EQ(checkpoint_before, ctx.local_.get_obj()->get_ddl_checkpoint_scn());
  for (int i = 0; i < 3; ++i) {
    ASSERT_FALSE(inc_kv_dag_finished(dag));
    EXPECT_EQ(OB_EAGAIN, ObDDLMergeScheduler::schedule_tablet_ddl_inc_major_merge(ctx.ls_, ctx.local_));
    EXPECT_EQ(before_create + 1, ddl_merge_dag_create_count);
  }
  // Bypassing the early check still returns EAGAIN, but performs preparation before final deduplication.
  EXPECT_EQ(OB_EAGAIN, compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(dag.ddl_param_));
  EXPECT_EQ(before_create + 2, ddl_merge_dag_create_count);
  first.get_obj()->dec_pending_cnt();
  pending = false;
  ASSERT_TRUE(wait_for_inc_kv_condition([&]() {
    return mgr.get_obj()->get_count() == 0 && inc_kv_dag_finished(dag);
  }));
  check_inc_kv_dump(ctx, trans_id, end_scn);
  ASSERT_EQ(OB_SUCCESS, mgr.get_obj()->get_or_create_inc_major_ddl_kv(
      SCN::plus(end_scn, 1), start_scn, ctx.local_.get_obj()->get_snapshot_version(), DATA_VERSION_4_6_3_0,
      trans_id, ObTxSEQ::from_seq(1), ObITable::INC_MAJOR_DDL_MEM_SSTABLE, next));
  ASSERT_TRUE(next.is_valid());
  EXPECT_EQ(end_scn, next.get_obj()->get_start_scn());
}

#endif

TEST_F(TestDliEndRecCheckpoint, primary_sql_keeps_data_checkpoint_for_main_and_lob)
{
  // Actual SQL validates the preserved producer order and final data coverage for main and LOB.
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  ObClusterVersion &version = ObClusterVersion::get_instance();
  const uint64_t saved_version = ATOMIC_LOAD(&version.data_version_);
  const uint64_t saved_mock = ATOMIC_LOAD(&ODV_MGR.mock_data_version_);
  DEFER({ version.update_data_version(saved_version); ODV_MGR.set_mock_data_version(saved_mock); });
  version.update_data_version(0);
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "create table rec_end_sql (a bigint primary key, b longtext) lob_inrow_threshold=0"));
  ASSERT_EQ(OB_SUCCESS, exe_sql(
      "insert /*+ direct(true, 0, 'inc_replace') enable_parallel_dml parallel(2) "
      "query_timeout(120000000) */ into rec_end_sql "
      "select row_number() over(), repeat('x', 9000) from table(generator(16))"));
  ObTabletID tablet_id;
  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, get_tablet_and_ls_id("rec_end_sql", tablet_id, ls_id));
  ObLSHandle ls;
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->get_ls(ls_id, ls, ObLSGetMod::STORAGE_MOD));
  ObTabletHandle local;
  ASSERT_EQ(OB_SUCCESS, ls.get_ls()->get_tablet(tablet_id, local));
  ObTabletBindingMdsUserData binding;
  ASSERT_EQ(OB_SUCCESS, local.get_obj()->ObITabletMdsInterface::get_ddl_data(
      SCN::max_scn(), binding));
  const ObTabletID targets[] = {
      tablet_id, binding.lob_meta_tablet_id_, binding.lob_piece_tablet_id_};
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_TRUE(targets[i].is_valid());
    ASSERT_EQ(OB_SUCCESS, ls.get_ls()->get_tablet(targets[i], local));
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "RecEndSql"));
    ObTabletHandle shared;
    ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->get_tablet(ls_id, targets[i],
        local.get_obj()->get_reorganization_scn(), allocator, shared));
    ObTabletMemberWrapper<ObTabletTableStore> store;
    ASSERT_EQ(OB_SUCCESS, shared.get_obj()->fetch_table_store_with_prewarm(store));
    ASSERT_GT(store.get_member()->get_inc_major_sstables().count(), 0);
    const ObSSTable *result = store.get_member()->get_inc_major_sstables().at(0);
    ASSERT_GE(shared.get_obj()->get_ddl_checkpoint_scn(), result->get_end_scn());
    ASSERT_GE(local.get_obj()->get_ddl_checkpoint_scn(), result->get_end_scn());
    ASSERT_GE(shared.get_obj()->get_tablet_meta().ddl_data_format_version_, DATA_VERSION_4_6_3_0);
    ObTransID trans_id;
    ObTxSEQ seq_no;
    ASSERT_EQ(OB_SUCCESS, compaction::ObIncMajorTxHelper::get_trans_id_and_seq_no(
        *result, trans_id, seq_no));
    ObTabletDDLCompleteMdsUserData completed;
    ASSERT_EQ(OB_SUCCESS, local.get_obj()->get_inc_major_direct_load_info(
        SCN::max_scn(), allocator, ObTabletDDLCompleteMdsUserDataKey(trans_id), completed));
    ASSERT_TRUE(completed.inc_major_commit_scn_.is_valid_and_not_min());
    ASSERT_LT(shared.get_obj()->get_ddl_checkpoint_scn(), completed.inc_major_commit_scn_);
    ASSERT_LT(local.get_obj()->get_ddl_checkpoint_scn(), completed.inc_major_commit_scn_);
  }
  ObMySQLProxy::MySQLResult result;
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().read(result,
      "select count(*) as cnt, sum(length(b)) as bytes from rec_end_sql"));
  ASSERT_EQ(OB_SUCCESS, result.get_result()->next());
  int64_t count = 0;
  int64_t bytes = 0;
  ASSERT_EQ(OB_SUCCESS, result.get_result()->get_int("cnt", count));
  ASSERT_EQ(OB_SUCCESS, result.get_result()->get_int("bytes", bytes));
  ASSERT_EQ(16, count);
  ASSERT_EQ(16 * 9000, bytes);
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table rec_end_sql"));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  const char *bucket = getenv("OB_MITTEST_S3_BUCKET");
  const char *endpoint = getenv("OB_MITTEST_S3_ENDPOINT");
  const char *access_id = getenv("OB_MITTEST_S3_AK");
  const char *access_key = getenv("OB_MITTEST_S3_SK");
  const char *region = getenv("OB_MITTEST_S3_REGION");
  char storage_info[1000] = {0};
  const int64_t prefix = ObTimeUtility::current_time_ns();
  const int format_ret = databuff_printf(storage_info, sizeof(storage_info),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s"
      "&max_iops=10000&max_bandwidth=200000000B&scope=region",
      nullptr == bucket ? oceanbase::unittest::S3_BUCKET : bucket, prefix,
      nullptr == endpoint ? oceanbase::unittest::S3_ENDPOINT : endpoint,
      nullptr == access_id ? oceanbase::unittest::S3_AK : access_id,
      nullptr == access_key ? oceanbase::unittest::S3_SK : access_key,
      nullptr == region ? oceanbase::unittest::S3_REGION : region);
  if (OB_SUCCESS != format_ret) {
    return format_ret;
  }
  oceanbase::shared_storage_info = storage_info;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");
  const int config_ret = GCONF.add_extra_config(oceanbase::unittest::SYS_CKPT_STARTUP_OPTION);
  if (OB_SUCCESS != config_ret) {
    return config_ret;
  }
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

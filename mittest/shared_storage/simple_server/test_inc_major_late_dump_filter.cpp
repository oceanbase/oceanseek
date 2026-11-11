/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#include "test_shared_meta_common.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_table_iterator.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#define private public
#define protected public
#include "storage/ddl/ob_ss_ddl_util.h"
#include "storage/incremental/ob_inc_sstable_upload_task.h"
#include "unittest/storage/init_basic_struct.h"
#include "storage/schema_utils.h"
#undef private
#undef protected

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
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

struct LateDumpContext
{
  LateDumpContext()
      : allocator_(ObMemAttr(MTL_ID(), "LateDumpTest")), schema_(nullptr) {}
  ~LateDumpContext() { ObTabletObjLoadHelper::free(allocator_, schema_); }
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

static int build_late_dump_sstable(ObArenaAllocator &allocator, const ObTabletID &tablet_id,
    const SCN &end_scn, const int64_t trans_id, const int64_t seq_no, ObSSTable &sstable,
    const ObITable::TableType table_type = ObITable::INC_MAJOR_SSTABLE,
    const int64_t upper_trans_version = INT64_MAX)
{
  int ret = OB_SUCCESS;
  ObTabletCreateSSTableParam param;
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
  param.ddl_scn_.convert_for_tx(SS_DDL_START_SCN_VAL);
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
  param.data_blocks_cnt_ = 0;
  param.micro_block_cnt_ = 0;
  param.use_old_macro_block_count_ = 0;
  param.row_count_ = 0;
  param.full_column_cnt_ = 0;
  param.occupy_size_ = 0;
  param.original_size_ = 0;
  param.recycle_version_ = 0;
  param.nested_offset_ = 0;
  param.nested_size_ = 0;
  if (!ObITable::is_major_sstable(table_type)) {
    param.uncommit_tx_info_.set_info_status_record_seq();
    // SSTable metadata stores the encoded transaction sequence, not a plain offset.
    ret = param.uncommit_tx_info_.push_back(compaction::ObUncommitTxDesc(
        trans_id, ObTxSEQ::from_seq(seq_no).cast_to_int()));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObSSTableMergeRes::fill_column_checksum_for_empty_major(
      param.column_cnt_, param.column_checksums_))) {
  } else if (OB_FAIL(sstable.init(param, &allocator))) {
  }
  return ret;
}

class TestIncMajorLateDumpFilter : public TestSharedMetaService
{
public:
  // Keep a separate data directory when CTest runs these fixtures concurrently.
  TestIncMajorLateDumpFilter()
    : TestSharedMetaService("test_inc_major_late_dump_filter")
  {}

protected:
  void SetUp() override
  {
    // Real bootstrap and tenant creation must retain normal writer qualifications.
    ATOMIC_STORE(&block_compaction_writer, false);
    TestSharedMetaService::SetUp();
    ATOMIC_STORE(&block_compaction_writer, true);
  }

  void TearDown() override
  {
    ATOMIC_STORE(&block_compaction_writer, false);
    TestSharedMetaService::TearDown();
  }

  int prepare(const char *name, LateDumpContext &ctx)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("create table %s (a int)", name))) {
    } else if (OB_FAIL(exe_sql(sql.ptr()))) {
    } else if (OB_FAIL(get_tablet_and_ls_id(name, ctx.tablet_id_, ctx.ls_id_))) {
    } else if (OB_FAIL(MTL(ObLSService *)->get_ls(
        ctx.ls_id_, ctx.ls_, ObLSGetMod::STORAGE_MOD))) {
    } else if (OB_FAIL(ctx.ls_.get_ls()->get_tablet(ctx.tablet_id_, ctx.local_))) {
    } else {
      ctx.reorganization_scn_ = ctx.local_.get_obj()->get_reorganization_scn();
      if (OB_FAIL(MTL(ObSSMetaService *)->create_tablet(
          ctx.ls_id_, ctx.tablet_id_, ctx.reorganization_scn_, true))) {
      } else if (OB_FAIL(ctx.local_.get_obj()->load_storage_schema(ctx.allocator_, ctx.schema_))) {
      } else if (OB_FAIL(ctx.rec_scn_.convert_for_tx(ObTimeUtility::current_time_ns()))) {
      } else {
        ctx.end_scn_ = SCN::plus(ctx.rec_scn_, 1000);
      }
    }
    return ret;
  }

  int latest(LateDumpContext &ctx, SCN *meta_scn = nullptr)
  {
    ctx.shared_.reset();
    SCN unused;
    return MTL(ObSSMetaService *)->get_tablet(ctx.ls_id_, ctx.tablet_id_,
        ctx.reorganization_scn_, ctx.allocator_, ctx.shared_,
        nullptr == meta_scn ? unused : *meta_scn);
  }

  int update_action(LateDumpContext &ctx, const ObSSTable &table,
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

  // Inspect the persisted protocol row, not just a successful API return.
  int tablet_protocol_states(LateDumpContext &ctx,
      std::map<uint64_t, ObSSMetaCtrlStatus::STATUS> &states)
  {
    states.clear();
    ObSSMetaCtrlValueIterator iter;
    int ret = iter.init(*GCTX.sql_proxy_, 10 * 1000 * 1000,
        run_ctx_.tenant_id_, ctx.ls_id_, ObString());
    ObSSMetaCtrlKey key;
    ObSSMetaCtrlValue value;
    while (OB_SUCC(ret) && OB_SUCC(ret = iter.get_next(key, value))) {
      if (blocksstable::ObStorageObjectType::SHARED_TABLET_META == key.type_
          && ctx.tablet_id_ == key.tablet_key_.tablet_id_
          && ctx.reorganization_scn_ == key.tablet_key_.reorg_scn_) {
        states.emplace(key.tablet_key_.control_scn_.get_val_for_tx(), value.ctrl_info_.status_.status_);
      }
    }
    return OB_ITER_END == ret ? OB_SUCCESS : ret;
  }
};

// DLI-FILTER-001/002: retained upload requests cannot republish covered dumps after ABORT;
// DLI-FILTER-003: a later, uncovered dump from another batch must still be published.
TEST_F(TestIncMajorLateDumpFilter, late_dump_is_filtered_after_inc_major_abort)
{
  share::ObTenantSwitchGuard tenant;
  ASSERT_EQ(OB_SUCCESS, tenant.switch_to(run_ctx_.tenant_id_));
  LateDumpContext ctx;
  ASSERT_EQ(OB_SUCCESS, prepare("inc_major_late_dump", ctx));
  ASSERT_EQ(OB_SUCCESS, latest(ctx));
  ObSSTable final_table;
  ASSERT_EQ(OB_SUCCESS, build_late_dump_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.rec_scn_, 77020, 20, final_table));
  const SCN dump_ends[] = {SCN::scn_dec(ctx.rec_scn_), ctx.rec_scn_};
  ObSSTable old_dumps[2];
  ObUpdateTableStoreParam pending_updates[2];
  ObMiniSSTableUploadTask uploader;
  uploader.param_.ls_id_ = ctx.ls_id_;
  ASSERT_EQ(OB_SUCCESS, uploader.param_.snapshot_version_.convert_for_tx(
      ctx.local_.get_obj()->get_snapshot_version()));
  for (int64_t i = 0; i < ARRAYSIZEOF(old_dumps); ++i) {
    ASSERT_EQ(OB_SUCCESS, build_late_dump_sstable(ctx.allocator_, ctx.tablet_id_,
        dump_ends[i], 77020, 20, old_dumps[i], ObITable::INC_MAJOR_DDL_DUMP_SSTABLE));
    ASSERT_LT(ctx.shared_.get_obj()->get_ddl_checkpoint_scn(), old_dumps[i].get_end_scn());
    bool is_stale = true;
    ASSERT_EQ(OB_SUCCESS, uploader.check_sstable_is_stale_(old_dumps[i].get_key(),
        *ctx.shared_.get_obj(), is_stale));
    ASSERT_FALSE(is_stale);
    // The source object and the request survive final publication and cleanup.
    // Do not re-run this precheck before submitting the retained request below.
    ASSERT_EQ(OB_SUCCESS, uploader.build_update_table_store_param_(ctx.shared_,
        *ctx.local_.get_obj(), old_dumps[i], *ctx.schema_, DATA_VERSION_4_6_3_0,
        false, ctx.allocator_, pending_updates[i]));
  }
  // This boundary test supplies the coverage promised by final publication.
  // It does not test the producer of this checkpoint (the separate MR 2906).
  ASSERT_EQ(OB_SUCCESS, update_action(ctx, final_table,
      ObIncMajorSSTableAction::ADD, ctx.rec_scn_));
  ASSERT_EQ(OB_SUCCESS, latest(ctx));
  ASSERT_EQ(ctx.rec_scn_, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
  ASSERT_EQ(DATA_VERSION_4_6_3_0, ctx.shared_.get_obj()->get_tablet_meta().ddl_data_format_version_);
  int64_t count = -1;
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.shared_, count));
  ASSERT_EQ(1, count);
  // Cleanup contributes no new coverage; it must retain the published checkpoint.
  ASSERT_EQ(OB_SUCCESS, update_action(ctx, final_table,
      ObIncMajorSSTableAction::ABORT, SCN::min_scn()));
  SCN cleaned_version;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &cleaned_version));
  ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.shared_, count));
  ASSERT_EQ(0, count);
  ASSERT_GE(ctx.shared_.get_obj()->get_ddl_checkpoint_scn(), ctx.rec_scn_);
  ObArray<ObSSMetaCtrlKeyInfo> keys;
  for (int64_t i = 0; i < ARRAYSIZEOF(old_dumps); ++i) {
    SCOPED_TRACE(i);
    std::map<uint64_t, ObSSMetaCtrlStatus::STATUS> before, after;
    ASSERT_EQ(OB_SUCCESS, tablet_protocol_states(ctx, before));
    ASSERT_EQ(&old_dumps[i], pending_updates[i].sstable_);
    // Continue after the already-passed precheck at the real shared API boundary.
    ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->update_tablet_table_store(ctx.ls_id_,
        ctx.tablet_id_, ctx.reorganization_scn_,
        ObMetaUpdateReason::TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE,
        pending_updates[i], keys, true));
    SCN retry_version;
    ASSERT_EQ(OB_SUCCESS, latest(ctx, &retry_version));
    ASSERT_EQ(cleaned_version, retry_version) << "covered dump persisted a new Shared Tablet";
    ASSERT_EQ(ctx.rec_scn_, ctx.shared_.get_obj()->get_ddl_checkpoint_scn());
    ASSERT_EQ(OB_SUCCESS, tablet_protocol_states(ctx, after));
    int64_t new_protocols = 0;
    for (const auto &entry : after) {
      if (0 == before.count(entry.first)) {
        ++new_protocols;
        EXPECT_EQ(ObSSMetaCtrlStatus::STATUS::ABANDON, entry.second);
      }
    }
    ASSERT_EQ(1, new_protocols) << "the covered request must finish its own atomic protocol";
    ObTableStoreIterator dumps;
    ASSERT_EQ(OB_SUCCESS, ctx.shared_.get_obj()->get_inc_major_ddl_sstables(dumps));
    ASSERT_EQ(0, dumps.count());
    ASSERT_EQ(OB_SUCCESS, inc_major_count(ctx.shared_, count));
    ASSERT_EQ(0, count);
  }
  ObSSTable next_dump;
  ASSERT_EQ(OB_SUCCESS, build_late_dump_sstable(ctx.allocator_, ctx.tablet_id_,
      ctx.end_scn_, 77021, 21, next_dump, ObITable::INC_MAJOR_DDL_DUMP_SSTABLE));
  ASSERT_LT(ctx.shared_.get_obj()->get_ddl_checkpoint_scn(), next_dump.get_end_scn());
  ObUpdateTableStoreParam next_update;
  ASSERT_EQ(OB_SUCCESS, uploader.build_update_table_store_param_(ctx.shared_,
      *ctx.local_.get_obj(), next_dump, *ctx.schema_, DATA_VERSION_4_6_3_0,
      false, ctx.allocator_, next_update));
  ASSERT_EQ(OB_SUCCESS, MTL(ObSSMetaService *)->update_tablet_table_store(ctx.ls_id_,
      ctx.tablet_id_, ctx.reorganization_scn_,
      ObMetaUpdateReason::TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE, next_update, keys, true));
  SCN next_version;
  ASSERT_EQ(OB_SUCCESS, latest(ctx, &next_version));
  ASSERT_GT(next_version, cleaned_version);
  ObTableStoreIterator next_dumps;
  ASSERT_EQ(OB_SUCCESS, ctx.shared_.get_obj()->get_inc_major_ddl_sstables(next_dumps));
  ASSERT_EQ(1, next_dumps.count());
  ASSERT_GE(ctx.shared_.get_obj()->get_ddl_checkpoint_scn(), next_dump.get_end_scn());
  ASSERT_EQ(OB_SUCCESS, exe_sql("drop table inc_major_late_dump"));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  // Allow runtime overrides of the shared S3 test configuration.
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

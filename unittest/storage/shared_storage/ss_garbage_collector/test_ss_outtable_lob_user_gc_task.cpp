/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include "lib/utility/ob_defer.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_table_schema.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/garbage_collector/ob_mark_sweep_pipeline.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/share/ob_ss_diagnose_mgr.h"
#include "storage/lob/store/ob_lob_outtable_io.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "storage/ob_storage_schema.h"
#include "storage/ss_garbage_collector/ob_ss_lob_gc_scan_util.h"
#include "storage/tablet/ob_tablet.h"
#define private public
#include "storage/ss_garbage_collector/ob_ss_mark_sweep_gc_rpc.h"
#include "storage/ss_garbage_collector/dag/ob_ss_mark_sweep_dag.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_outtable_gc_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_meta_gc_scan_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_scan_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_merge_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_diff_task.h"
#include "storage/ss_garbage_collector/user_data_gc_dag_task/ob_ss_lob_ctrl_gc_delete_task.h"
#undef private

namespace oceanbase
{
namespace storage
{

static thread_local const ObSSGCTablet *mock_latest_gc_tablet = nullptr;
static thread_local const ObTablet *mock_latest_tablet = nullptr;
static thread_local share::SCN mock_latest_meta_scn;

// Override this shared-library symbol only in the test executable.
int ObSSMetaService::get_tablet(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const share::SCN &reorgan_scn,
    const share::SCN &meta_scn,
    common::ObArenaAllocator &allocator,
    ObTabletHandle &tablet_hdl)
{
  int ret = common::OB_NOT_INIT;
  if (nullptr != mock_latest_tablet) {
    if (nullptr == mock_latest_gc_tablet
        || ls_id != mock_latest_gc_tablet->ls_id_
        || tablet_id != mock_latest_gc_tablet->tablet_id_
        || reorgan_scn != mock_latest_gc_tablet->transfer_scn_
        || meta_scn != mock_latest_meta_scn) {
      ret = common::OB_INVALID_ARGUMENT;
    } else {
      void *buf = allocator.alloc(sizeof(ObTablet));
      if (nullptr == buf) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
      } else {
        // The handle owns this copy; the fixture's stack tablet must remain untouched.
        ObTablet *tablet = new (buf) ObTablet();
        tablet->set_allocator(&allocator);
        ObTabletMeta &dst = const_cast<ObTabletMeta &>(tablet->get_tablet_meta());
        const ObTabletMeta &src = mock_latest_tablet->get_tablet_meta();
        dst.ls_id_ = ls_id;
        dst.tablet_id_ = tablet_id;
        dst.clog_checkpoint_scn_ = src.clog_checkpoint_scn_;
        dst.ha_status_ = src.ha_status_;
        dst.split_info_ = src.split_info_;
        dst.fork_info_ = src.fork_info_;
        dst.transfer_info_ = src.transfer_info_;
        dst.is_empty_shell_ = src.is_empty_shell_;
        tablet_hdl.set_obj(ObTabletHandle::ObTabletHdlType::STANDALONE,
            tablet, &allocator, nullptr);
        ret = common::OB_SUCCESS;
      }
    }
  }
  return ret;
}

} // namespace storage

namespace unittest
{

using namespace common;
using namespace share;
using namespace storage;

class TestSSOuttableLobUserGCTask : public ::testing::Test
{
public:
  TestSSOuttableLobUserGCTask()
    : tenant_(OB_SYS_TENANT_ID), previous_tenant_(nullptr), previous_cluster_version_(0)
  {}

  void SetUp() override
  {
    previous_tenant_ = ObTenantEnv::get_tenant();
    previous_cluster_version_ = GET_MIN_CLUSTER_VERSION();
    ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_6_3_1);
    tenant_.set_unit_memory_size(1LL << 30);
    ObTenantEnv::set_tenant(&tenant_);
  }

  void TearDown() override
  {
    ObTenantEnv::set_tenant(previous_tenant_);
    ObClusterVersion::get_instance().update_cluster_version(previous_cluster_version_);
  }

protected:
  int init_complete_tablet(
      ObTablet &tablet,
      const SCN &checkpoint)
  {
    ObTabletMeta &meta = const_cast<ObTabletMeta &>(tablet.get_tablet_meta());
    meta.clog_checkpoint_scn_ = checkpoint;
    return meta.ha_status_.init_status();
  }

  int calc_checkpoint_with_latest(
      const ObSSGCTabletWithMetaScn &input,
      const ObTablet &first,
      const ObTablet &latest,
      SCN &checkpoint)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator;
    ObSSMetaService meta_service;
    ObSSOuttableGCTask task;
    ObSEArray<SCN, 2> kept_meta_scns;
    SCN latest_meta_scn;
    ObTenantBase *tenant = ObTenantEnv::get_tenant();
    if (nullptr == tenant) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(latest_meta_scn.convert_for_tx(2))) {
    } else if (OB_FAIL(kept_meta_scns.push_back(SCN::base_scn()))) {
    } else if (OB_FAIL(kept_meta_scns.push_back(latest_meta_scn))) {
    } else {
      ObSSMetaService *previous_service = tenant->get<ObSSMetaService *>();
      const ObSSGCTablet *previous_gc_tablet = mock_latest_gc_tablet;
      const ObTablet *previous_latest = mock_latest_tablet;
      const SCN previous_meta_scn = mock_latest_meta_scn;
      DEFER(mock_latest_gc_tablet = previous_gc_tablet;
            mock_latest_tablet = previous_latest;
            mock_latest_meta_scn = previous_meta_scn;
            tenant->set(previous_service);
            ObTenantEnv::set_tenant(tenant));
      mock_latest_gc_tablet = &input.gc_tablet_;
      mock_latest_tablet = &latest;
      mock_latest_meta_scn = latest_meta_scn;
      tenant->set(&meta_service);
      ObTenantEnv::set_tenant(tenant);
      ret = task.calc_lob_gc_checkpoint_scn_(input, first, kept_meta_scns, allocator, checkpoint);
    }
    return ret;
  }

  int init_param(ObSSMarkSweepDagParam &param)
  {
    int ret = param.init(OB_SYS_TENANT_ID, 1, ObSSMarkSweepDagParam::DEFAULT_MEMORY_LIMIT,
        ObSSMarkSweepDagParam::DEFAULT_FILE_BUF_SIZE, ObSSMarkSweepDagParam::DEFAULT_BATCH_SIZE);
    if (OB_SUCC(ret)) {
      param.snapshot_meta_scn_ = SCN::base_scn();
      param.safe_meta_scn_ = SCN::base_scn();
      ret = param.tablet_with_meta_scn_arr_.push_back(ObSSGCTabletWithMetaScn(
          ObSSGCTablet(ObLSID(1), ObTabletID(200001), SCN::base_scn()), SCN::base_scn(),
          ObSSGCTabletType::UNKNOWN));
    }
    return ret;
  }

  int init_scan_inputs(ObSSMarkSweepCtx &ctx, const int64_t count)
  {
    int ret = ctx.min_outtable_data_scn_.convert_for_tx(100);
    ctx.lob_meta_scan_inputs_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ret = ctx.lob_meta_scan_inputs_.push_back(ObSSGCTabletWithMetaScn(
          ObSSGCTablet(ObLSID(1), ObTabletID(200001 + i), SCN::base_scn()), SCN::base_scn(),
          ObSSGCTabletType::UNKNOWN));
    }
    return ret;
  }

  int init_ctrl_scan_inputs(ObSSMarkSweepCtx &ctx, const int64_t count)
  {
    int ret = ctx.min_outtable_data_scn_.convert_for_tx(100);
    ctx.lob_ctrl_scan_inputs_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ret = ctx.lob_ctrl_scan_inputs_.push_back(ObSSGCTabletWithMetaScn(
          ObSSGCTablet(ObLSID(1), ObTabletID(400001 + i), SCN::base_scn()), SCN::base_scn(),
          ObSSGCTabletType::UNKNOWN));
    }
    return ret;
  }

  int init_ctrl_schema(schema::ObTableSchema &table_schema, const int64_t partition_count)
  {
    int ret = OB_SUCCESS;
    table_schema.reset();
    table_schema.set_tenant_id(OB_SYS_TENANT_ID);
    table_schema.set_table_id(500001);
    table_schema.set_table_type(schema::AUX_OUTTABLE_LOB_CONTROL);
    table_schema.set_part_level(schema::PARTITION_LEVEL_ONE);
    table_schema.set_part_num(partition_count);
    table_schema.get_part_option().set_part_func_type(schema::PARTITION_FUNC_TYPE_HASH);
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_count; ++i) {
      schema::ObPartition partition;
      partition.set_tenant_id(OB_SYS_TENANT_ID);
      partition.set_table_id(500001);
      partition.set_part_id(i);
      partition.set_part_idx(i);
      partition.set_tablet_id(200001 + i);
      ret = table_schema.add_partition(partition);
    }
    return ret;
  }

  int prepare_ctrl_inputs_for_role(const ObTenantRole::Role role, ObTabletIDArray &scan_inputs)
  {
    ObTenantBase user_tenant(1002);
    user_tenant.set_tenant_role(role);
    ObTenantBase *previous_tenant = ObTenantEnv::get_tenant();
    ObTenantEnv::set_tenant(&user_tenant);
    ObSSOuttableGCTask task;
    const int ret = task.prepare_lob_ctrl_scan_inputs_(user_tenant.id(), scan_inputs);
    ObTenantEnv::set_tenant(previous_tenant);
    return ret;
  }

private:
  ObTenantBase tenant_;
  ObTenantBase *previous_tenant_;
  uint64_t previous_cluster_version_;
};

TEST_F(TestSSOuttableLobUserGCTask, default_and_reset)
{
  ObSSMarkSweepCtx ctx;
  ASSERT_TRUE(ctx.min_outtable_data_scn_.is_max());
  ASSERT_TRUE(ctx.lob_meta_scan_inputs_.empty());
  ASSERT_TRUE(ctx.lob_ctrl_scan_inputs_.empty());
  ASSERT_FALSE(ctx.lob_ctrl_merged_file_.is_valid());
  ASSERT_FALSE(ctx.lob_ctrl_diff_file_.is_valid());
  ASSERT_FALSE(ctx.is_valid());
  ASSERT_EQ(OB_SUCCESS, ctx.min_outtable_data_scn_.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, init_scan_inputs(ctx, 2));
  ASSERT_EQ(OB_SUCCESS, init_ctrl_scan_inputs(ctx, 1));
  ctx.reset();
  ASSERT_TRUE(ctx.min_outtable_data_scn_.is_max());
  ASSERT_TRUE(ctx.lob_meta_scan_inputs_.empty());
  ASSERT_TRUE(ctx.lob_ctrl_scan_inputs_.empty());
}

TEST_F(TestSSOuttableLobUserGCTask, default_max_keeps_system_gc_context_valid)
{
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ASSERT_TRUE(param.is_valid());
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_NE(nullptr, dag.get_ctx());
  ASSERT_TRUE(dag.get_ctx()->is_valid());
  ASSERT_TRUE(dag.get_ctx()->min_outtable_data_scn_.is_max());
}

TEST_F(TestSSOuttableLobUserGCTask, dag_param_arrays_are_copied)
{
  ObSSMarkSweepDag dag;
  SCN effective_checkpoint;
  ASSERT_EQ(OB_SUCCESS, effective_checkpoint.convert_for_tx(200));
  const SCN original_checkpoint = SCN::base_scn();
  {
    ObSSMarkSweepDagParam param;
    ASSERT_EQ(OB_SUCCESS, init_param(param));
    param.tablet_with_meta_scn_arr_.at(0).clog_checkpoint_scn_ = effective_checkpoint;
    param.tablet_with_meta_scn_arr_.at(0).orig_clog_checkpoint_scn_ = original_checkpoint;
    ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
    param.tablet_with_meta_scn_arr_.reset();
  }
  ASSERT_NE(nullptr, dag.get_ctx());
  ASSERT_TRUE(dag.get_ctx()->is_valid());
  ASSERT_TRUE(dag.get_ctx()->min_outtable_data_scn_.is_max());
  ASSERT_EQ(1, dag.get_ctx()->tablet_with_meta_scn_arr_.count());
  ASSERT_EQ(ObTabletID(200001), dag.get_ctx()->tablet_with_meta_scn_arr_.at(0).gc_tablet_.tablet_id_);
  const ObSSGCTabletWithMetaScn &copied = dag.get_ctx()->tablet_with_meta_scn_arr_.at(0);
  ASSERT_EQ(effective_checkpoint, copied.clog_checkpoint_scn_);
  ASSERT_EQ(original_checkpoint, copied.orig_clog_checkpoint_scn_);
}

TEST_F(TestSSOuttableLobUserGCTask, lob_checkpoint_uses_study_and_shared_versions)
{
  struct CheckpointCase
  {
    int64_t first_;
    int64_t latest_;
    int64_t effective_;
    int64_t original_;
    int64_t expected_;
  };
  const CheckpointCase cases[] = {
      {1, 1, 200, 1, 200},       // Initial checkpoints must not pin a cold tablet.
      {1, 1, 200, 100, 1},       // Shared data is behind the local original checkpoint.
      {150, 150, 200, 100, 200}, // The shared baseline lies inside the cold interval.
      {150, 150, 200, 1, 200},   // An asymmetric replica may keep its original checkpoint at 1.
      {100, 100, 200, 100, 200}, // The original equality case must still advance.
      {100, 150, 200, 100, 100}, // Shared checkpoints changed within the kept versions.
      {150, 175, 200, 100, 100}, // Keep the frozen first/latest checkpoint consistency guard.
      {200, 200, 200, 100, 100}, // This promotion only applies below the effective checkpoint.
      {250, 250, 200, 100, 100}, // Do not extend the cold interval beyond Study's boundary.
      {150, 150, 100, 100, 100}, // Equal original and effective checkpoints are warm.
      {100, 100, 80, 100, 80},   // A lower effective checkpoint remains a constraint.
      {100, 100, 0, 100, 0},     // A MIN Study result must prevent user GC.
  };
  const ObSSGCTablet gc_tablet(ObLSID(1), ObTabletID(200001), SCN::base_scn());
  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    SCOPED_TRACE(i);
    SCN first_checkpoint;
    SCN latest_checkpoint;
    SCN effective_checkpoint;
    SCN original_checkpoint;
    SCN expected_checkpoint;
    ASSERT_EQ(OB_SUCCESS, first_checkpoint.convert_for_tx(cases[i].first_));
    ASSERT_EQ(OB_SUCCESS, latest_checkpoint.convert_for_tx(cases[i].latest_));
    ASSERT_EQ(OB_SUCCESS, effective_checkpoint.convert_for_tx(cases[i].effective_));
    ASSERT_EQ(OB_SUCCESS, original_checkpoint.convert_for_tx(cases[i].original_));
    ASSERT_EQ(OB_SUCCESS, expected_checkpoint.convert_for_tx(cases[i].expected_));
    ObTablet first;
    ObTablet latest;
    ASSERT_EQ(OB_SUCCESS, init_complete_tablet(first, first_checkpoint));
    ASSERT_EQ(OB_SUCCESS, init_complete_tablet(latest, latest_checkpoint));
    const ObSSGCTabletWithMetaScn input(gc_tablet, SCN::base_scn(), ObSSGCTabletType::UNKNOWN,
        effective_checkpoint, original_checkpoint);
    SCN checkpoint;
    ASSERT_EQ(OB_SUCCESS, calc_checkpoint_with_latest(input, first, latest, checkpoint));
    ASSERT_EQ(expected_checkpoint, checkpoint);
  }
}

TEST_F(TestSSOuttableLobUserGCTask, lob_checkpoint_single_kept_version_can_advance)
{
  ObSSOuttableGCTask task;
  ObTablet tablet;
  ASSERT_EQ(OB_SUCCESS, init_complete_tablet(tablet, SCN::base_scn()));
  SCN effective_checkpoint;
  ASSERT_EQ(OB_SUCCESS, effective_checkpoint.convert_for_tx(200));
  const ObSSGCTabletWithMetaScn input(ObSSGCTablet(ObLSID(1), ObTabletID(200001), SCN::base_scn()),
      SCN::base_scn(), ObSSGCTabletType::UNKNOWN, effective_checkpoint, SCN::base_scn());
  SCN checkpoint;
  ObArenaAllocator allocator;
  ObSEArray<SCN, 1> kept_meta_scns;
  ASSERT_EQ(OB_SUCCESS, kept_meta_scns.push_back(SCN::base_scn()));
  ASSERT_EQ(OB_SUCCESS,
      task.calc_lob_gc_checkpoint_scn_(input, tablet, kept_meta_scns, allocator, checkpoint));
  ASSERT_EQ(effective_checkpoint, checkpoint);
}

TEST_F(TestSSOuttableLobUserGCTask, lob_checkpoint_advances_after_asymmetric_replica_study)
{
  struct ReplicaCase
  {
    int64_t first_original_;
    int64_t second_original_;
    int64_t ls_checkpoint_;
    int64_t shared_checkpoint_;
    bool first_has_memtable_;
    int64_t expected_;
  };
  const ReplicaCase cases[] = {
      // Round 11: both replicas are cold, but only z1 flushed an empty MemTable.
      {1794298319882385000, 1, 1794300556646226000, 1794298319882385000,
          false, 1794300556646226000},
      // The replica supplying orig_min is cold; the other replica still limits effective_min.
      {175, 100, 200, 150, true, 175},
  };
  const ObSSGCTablet gc_tablet(ObLSID(1), ObTabletID(200001), SCN::base_scn());
  obrpc::ObSSMarkSweepGCStudyP processor;
  ObSSOuttableGCTask task;
  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    SCOPED_TRACE(i);
    SCN originals[2];
    SCN ls_checkpoint;
    SCN shared_checkpoint;
    SCN expected_checkpoint;
    ASSERT_EQ(OB_SUCCESS, originals[0].convert_for_tx(cases[i].first_original_));
    ASSERT_EQ(OB_SUCCESS, originals[1].convert_for_tx(cases[i].second_original_));
    ASSERT_EQ(OB_SUCCESS, ls_checkpoint.convert_for_tx(cases[i].ls_checkpoint_));
    ASSERT_EQ(OB_SUCCESS, shared_checkpoint.convert_for_tx(cases[i].shared_checkpoint_));
    ASSERT_EQ(OB_SUCCESS, expected_checkpoint.convert_for_tx(cases[i].expected_));
    for (int order = 0; order < 2; ++order) {
      SCOPED_TRACE(order);
      ObSSGCTabletStudyResultArray server_result;
      ObSSGCTabletStudyResultArray final_result;
      hash::ObHashMap<ObSSGCTablet, int64_t> tablet_index_map;
      ASSERT_EQ(OB_SUCCESS, tablet_index_map.create(8, ObMemAttr(OB_SYS_TENANT_ID, "SSGCStudyUT")));
      for (int j = 0; j < 2; ++j) {
        const int replica = (order + j) % 2;
        const bool has_memtable = 0 == replica && cases[i].first_has_memtable_;
        ObSSGCTabletStudyInfo info(SCN::max_scn(), 10, originals[replica], 20);
        ASSERT_EQ(!has_memtable, processor.update_cold_lob_rely_info_(
            true, 1, has_memtable, ls_checkpoint, info));
        ASSERT_EQ(originals[replica], info.orig_clog_checkpoint_scn_);
        server_result.reuse();
        ASSERT_EQ(OB_SUCCESS, server_result.push_back(
            ObSSGCTabletStudyResult(gc_tablet, info, ObSSGCTabletType::LOB_META)));
        ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
            server_result, tablet_index_map, final_result));
      }
      ASSERT_EQ(1, final_result.count());
      const ObSSGCTabletStudyInfo &info = final_result.at(0).study_info_;
      ASSERT_TRUE(info.is_cold());
      ASSERT_EQ(originals[1], info.orig_clog_checkpoint_scn_);
      ASSERT_EQ(expected_checkpoint, info.clog_checkpoint_scn_);
      const ObSSGCTabletWithMetaScn input(gc_tablet, SCN::base_scn(), ObSSGCTabletType::LOB_META,
          info.clog_checkpoint_scn_, info.orig_clog_checkpoint_scn_);
      ObTablet tablet;
      ASSERT_EQ(OB_SUCCESS, init_complete_tablet(tablet, shared_checkpoint));
      ObArenaAllocator allocator;
      ObSEArray<SCN, 1> kept_meta_scns;
      ASSERT_EQ(OB_SUCCESS, kept_meta_scns.push_back(SCN::base_scn()));
      SCN checkpoint;
      ASSERT_EQ(OB_SUCCESS,
          task.calc_lob_gc_checkpoint_scn_(input, tablet, kept_meta_scns, allocator, checkpoint));
      EXPECT_EQ(expected_checkpoint, checkpoint);
      ASSERT_EQ(OB_SUCCESS, calc_checkpoint_with_latest(input, tablet, tablet, checkpoint));
      EXPECT_EQ(expected_checkpoint, checkpoint);
    }
  }
}

TEST_F(TestSSOuttableLobUserGCTask, lob_checkpoint_loads_latest_only_for_promotion)
{
  ObSSOuttableGCTask task;
  ObTablet tablet;
  ASSERT_EQ(OB_SUCCESS, init_complete_tablet(tablet, SCN::base_scn()));
  SCN effective_checkpoint;
  ASSERT_EQ(OB_SUCCESS, effective_checkpoint.convert_for_tx(200));
  ObSSGCTabletWithMetaScn input(ObSSGCTablet(ObLSID(1), ObTabletID(200001), SCN::base_scn()),
      SCN::base_scn(), ObSSGCTabletType::UNKNOWN, effective_checkpoint, SCN::base_scn());
  ObSEArray<SCN, 2> kept_meta_scns;
  ObArenaAllocator allocator;
  SCN checkpoint;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      task.calc_lob_gc_checkpoint_scn_(input, tablet, kept_meta_scns, allocator, checkpoint));
  ASSERT_TRUE(checkpoint.is_min());
  ASSERT_EQ(OB_SUCCESS, kept_meta_scns.push_back(SCN::base_scn()));
  ASSERT_EQ(OB_SUCCESS, kept_meta_scns.push_back(effective_checkpoint));
  ObSSMetaService meta_service;
  ObSSDiagnoseInfoMgr diagnose_mgr;
  ObTenantBase *tenant = ObTenantEnv::get_tenant();
  ASSERT_NE(nullptr, tenant);
  ObSSMetaService *previous_meta_service = tenant->get<ObSSMetaService *>();
  ObSSDiagnoseInfoMgr *previous_diagnose_mgr = tenant->get<ObSSDiagnoseInfoMgr *>();
  DEFER(tenant->set(previous_meta_service);
        tenant->set(previous_diagnose_mgr);
        ObTenantEnv::set_tenant(tenant));
  tenant->set<ObSSMetaService *>(nullptr);
  ObTenantEnv::set_tenant(tenant);
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      task.calc_lob_gc_checkpoint_scn_(input, tablet, kept_meta_scns, allocator, checkpoint));
  ASSERT_TRUE(checkpoint.is_min());
  // Paths that cannot promote must not require the metadata service.
  input.clog_checkpoint_scn_ = SCN::base_scn();
  ASSERT_EQ(OB_SUCCESS,
      task.calc_lob_gc_checkpoint_scn_(input, tablet, kept_meta_scns, allocator, checkpoint));
  ASSERT_EQ(SCN::base_scn(), checkpoint);
  input.clog_checkpoint_scn_ = effective_checkpoint;
  ObTabletMeta &meta = const_cast<ObTabletMeta &>(tablet.get_tablet_meta());
  meta.split_info_.set_data_incomplete(true);
  ASSERT_EQ(OB_SUCCESS,
      task.calc_lob_gc_checkpoint_scn_(input, tablet, kept_meta_scns, allocator, checkpoint));
  ASSERT_TRUE(checkpoint.is_min());
  meta.split_info_.set_data_incomplete(false);
  tenant->set(&meta_service);
  tenant->set(&diagnose_mgr);
  ObTenantEnv::set_tenant(tenant);
  // A required load failure must be propagated without promoting the boundary.
  ASSERT_EQ(OB_NOT_INIT,
      task.calc_lob_gc_checkpoint_scn_(input, tablet, kept_meta_scns, allocator, checkpoint));
  ASSERT_TRUE(checkpoint.is_min());
}

TEST_F(TestSSOuttableLobUserGCTask, lob_checkpoint_missing_study_keeps_shared_boundary)
{
  ObTablet first;
  ObTablet latest;
  ASSERT_EQ(OB_SUCCESS, init_complete_tablet(first, SCN::base_scn()));
  ASSERT_EQ(OB_SUCCESS, init_complete_tablet(latest, SCN::base_scn()));
  const ObSSGCTabletWithMetaScn input(ObSSGCTablet(ObLSID(1), ObTabletID(200001), SCN::base_scn()),
      SCN::base_scn(), ObSSGCTabletType::UNKNOWN);
  ASSERT_TRUE(input.clog_checkpoint_scn_.is_max());
  ASSERT_TRUE(input.orig_clog_checkpoint_scn_.is_max());
  SCN checkpoint;
  ASSERT_EQ(OB_SUCCESS, calc_checkpoint_with_latest(input, first, latest, checkpoint));
  ASSERT_EQ(SCN::base_scn(), checkpoint);
}

TEST_F(TestSSOuttableLobUserGCTask, lob_checkpoint_incomplete_first_blocks_user_gc)
{
  SCN original_checkpoint;
  SCN shared_checkpoint;
  SCN effective_checkpoint;
  ASSERT_EQ(OB_SUCCESS, original_checkpoint.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, shared_checkpoint.convert_for_tx(150));
  ASSERT_EQ(OB_SUCCESS, effective_checkpoint.convert_for_tx(200));
  const ObSSGCTabletWithMetaScn input(ObSSGCTablet(ObLSID(1), ObTabletID(200001), SCN::base_scn()),
      SCN::base_scn(), ObSSGCTabletType::UNKNOWN, effective_checkpoint, original_checkpoint);
  for (int mode = 0; mode < 3; ++mode) {
    SCOPED_TRACE(mode);
    ObTablet first;
    ObTablet latest;
    ASSERT_EQ(OB_SUCCESS, init_complete_tablet(first, shared_checkpoint));
    ASSERT_EQ(OB_SUCCESS, init_complete_tablet(latest, shared_checkpoint));
    ObTabletMeta &first_meta = const_cast<ObTabletMeta &>(first.get_tablet_meta());
    ObTabletMeta &latest_meta = const_cast<ObTabletMeta &>(latest.get_tablet_meta());
    if (0 == mode) {
      first_meta.fork_info_.set_fork_snapshot_version(100);
      first_meta.fork_info_.set_src_tablet_id(ObTabletID(200002));
      first_meta.fork_info_.set_data_complete(false);
      ASSERT_EQ(OB_SUCCESS, latest_meta.fork_info_.assign(first_meta.fork_info_));
      latest_meta.fork_info_.set_data_complete(true);
    } else if (1 == mode) {
      first_meta.split_info_.set_data_incomplete(true);
    } else {
      ASSERT_EQ(OB_SUCCESS, first_meta.ha_status_.set_data_status(ObTabletDataStatus::INCOMPLETE));
    }
    ASSERT_TRUE(latest.is_data_complete());
    SCN checkpoint;
    ASSERT_EQ(OB_SUCCESS, calc_checkpoint_with_latest(input, first, latest, checkpoint));
    ASSERT_TRUE(checkpoint.is_min());
  }
}

TEST_F(TestSSOuttableLobUserGCTask, lob_checkpoint_incomplete_latest_prevents_promotion)
{
  SCN original_checkpoint;
  SCN shared_checkpoint;
  SCN effective_checkpoint;
  ASSERT_EQ(OB_SUCCESS, original_checkpoint.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, shared_checkpoint.convert_for_tx(150));
  ASSERT_EQ(OB_SUCCESS, effective_checkpoint.convert_for_tx(200));
  const ObSSGCTabletWithMetaScn input(ObSSGCTablet(ObLSID(1), ObTabletID(200001), SCN::base_scn()),
      SCN::base_scn(), ObSSGCTabletType::UNKNOWN, effective_checkpoint, original_checkpoint);
  for (int mode = 0; mode < 4; ++mode) {
    SCOPED_TRACE(mode);
    ObTablet first;
    ObTablet latest;
    ASSERT_EQ(OB_SUCCESS, init_complete_tablet(first, shared_checkpoint));
    ASSERT_EQ(OB_SUCCESS, init_complete_tablet(latest, shared_checkpoint));
    ObTabletMeta &latest_meta = const_cast<ObTabletMeta &>(latest.get_tablet_meta());
    if (0 == mode) {
      latest_meta.fork_info_.set_fork_snapshot_version(100);
      latest_meta.fork_info_.set_src_tablet_id(ObTabletID(200002));
      latest_meta.fork_info_.set_data_complete(false);
    } else if (1 == mode) {
      latest_meta.split_info_.set_data_incomplete(true);
    } else if (2 == mode) {
      latest_meta.is_empty_shell_ = true;
      latest_meta.clog_checkpoint_scn_.reset();
    } else {
      ASSERT_EQ(OB_SUCCESS, latest_meta.ha_status_.set_data_status(ObTabletDataStatus::INCOMPLETE));
    }
    SCN checkpoint;
    ASSERT_EQ(OB_SUCCESS, calc_checkpoint_with_latest(input, first, latest, checkpoint));
    ASSERT_EQ(original_checkpoint, checkpoint);
  }
}

TEST_F(TestSSOuttableLobUserGCTask, lob_checkpoint_rejects_invalid_boundaries)
{
  SCN original_checkpoint;
  SCN shared_checkpoint;
  SCN effective_checkpoint;
  ASSERT_EQ(OB_SUCCESS, original_checkpoint.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, shared_checkpoint.convert_for_tx(150));
  ASSERT_EQ(OB_SUCCESS, effective_checkpoint.convert_for_tx(200));
  const ObSSGCTablet gc_tablet(ObLSID(1), ObTabletID(200001), SCN::base_scn());
  for (int mode = 0; mode < 5; ++mode) {
    SCOPED_TRACE(mode);
    ObTablet first;
    ObTablet latest;
    ASSERT_EQ(OB_SUCCESS, init_complete_tablet(first, shared_checkpoint));
    ASSERT_EQ(OB_SUCCESS, init_complete_tablet(latest, shared_checkpoint));
    ObSSGCTabletWithMetaScn input(gc_tablet, SCN::base_scn(), ObSSGCTabletType::UNKNOWN,
        effective_checkpoint, original_checkpoint);
    ObTabletMeta &first_meta = const_cast<ObTabletMeta &>(first.get_tablet_meta());
    ObTabletMeta &latest_meta = const_cast<ObTabletMeta &>(latest.get_tablet_meta());
    if (0 == mode) {
      first_meta.clog_checkpoint_scn_.reset();
    } else if (1 == mode) {
      first_meta.clog_checkpoint_scn_.set_max();
    } else if (2 == mode) {
      input.clog_checkpoint_scn_.reset();
    } else if (3 == mode) {
      input.orig_clog_checkpoint_scn_.reset();
    } else {
      latest_meta.clog_checkpoint_scn_.reset();
    }
    SCN checkpoint;
    ASSERT_NE(OB_SUCCESS, calc_checkpoint_with_latest(input, first, latest, checkpoint));
  }
}

TEST_F(TestSSOuttableLobUserGCTask, min_scn_keeps_system_gc_context_valid)
{
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ASSERT_TRUE(param.is_valid());
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_NE(nullptr, dag.get_ctx());
  dag.get_ctx()->min_outtable_data_scn_.set_min();
  ASSERT_TRUE(dag.get_ctx()->is_valid());
  ASSERT_TRUE(dag.get_ctx()->min_outtable_data_scn_.is_min());
}

TEST_F(TestSSOuttableLobUserGCTask, reject_invalid_param)
{
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  param.snapshot_meta_scn_.reset();
  ASSERT_FALSE(param.is_valid());
  ObSSMarkSweepCtx ctx;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ctx.init_by_param(param));
  ASSERT_FALSE(ctx.is_valid());
  ASSERT_TRUE(ctx.min_outtable_data_scn_.is_max());
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_INVALID_ARGUMENT, dag.init_by_param(&param));
  ASSERT_EQ(nullptr, dag.get_ctx());
}

TEST_F(TestSSOuttableLobUserGCTask, task_initialization_and_invalid_context)
{
  ObSSOuttableGCTask task;
  ASSERT_EQ(OB_NOT_INIT, task.process());
  ASSERT_EQ(OB_SUCCESS, task.init());
  ASSERT_EQ(OB_INIT_TWICE, task.init());
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_NE(nullptr, dag.get_ctx());
  dag.get_ctx()->min_outtable_data_scn_.reset();
  ASSERT_FALSE(dag.get_ctx()->is_valid());
  task.set_dag(dag);
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
}

TEST_F(TestSSOuttableLobUserGCTask, empty_scan_input_resets_outputs)
{
  ObSSMarkSweepCtx ctx;
  ObSSOuttableGCTask task;
  ObSSGCTabletWithMetaScnArray scan_inputs;
  ObSSGCTabletWithMetaScnArray ctrl_scan_inputs;
  ASSERT_EQ(OB_SUCCESS, ctrl_scan_inputs.push_back(ObSSGCTabletWithMetaScn(
      ObSSGCTablet(ObLSID(1), ObTabletID(200002), SCN::base_scn()), SCN::base_scn(),
      ObSSGCTabletType::UNKNOWN)));
  ASSERT_EQ(OB_SUCCESS, scan_inputs.push_back(ObSSGCTabletWithMetaScn(
      ObSSGCTablet(ObLSID(1), ObTabletID(200001), SCN::base_scn()), SCN::base_scn(),
      ObSSGCTabletType::UNKNOWN)));
  SCN min_outtable_data_scn;
  ASSERT_EQ(OB_SUCCESS, min_outtable_data_scn.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS,
      task.prepare_scan_inputs_(ctx, scan_inputs, ctrl_scan_inputs, min_outtable_data_scn));
  ASSERT_TRUE(scan_inputs.empty());
  ASSERT_TRUE(ctrl_scan_inputs.empty());
  ASSERT_TRUE(min_outtable_data_scn.is_max());
}

TEST_F(TestSSOuttableLobUserGCTask, normal_tablet_skips_shared_metadata)
{
  ObSSOuttableGCTask task;
  ObSSMarkSweepCtx ctx;
  ctx.tenant_id_ = OB_SYS_TENANT_ID;
  ObSSGCTabletWithMetaScn input(ObSSGCTablet(ObLSID(1), ObTabletID(200001), SCN::base_scn()),
      SCN::base_scn(), ObSSGCTabletType::NORMAL);
  ObSSGCTabletWithMetaScnArray meta_inputs;
  ObSSGCTabletWithMetaScnArray ctrl_inputs;
  SCN checkpoint = SCN::max_scn();
  // No shared metadata service is installed: NORMAL must avoid even enumerating versions.
  ASSERT_EQ(OB_SUCCESS,
      task.prepare_lob_tablet_scan_input_(ctx, input, meta_inputs, ctrl_inputs, checkpoint));
  ASSERT_TRUE(meta_inputs.empty());
  ASSERT_TRUE(ctrl_inputs.empty());
  ASSERT_TRUE(checkpoint.is_max());
  const ObSSGCTabletType scan_types[] = {
      ObSSGCTabletType::UNKNOWN, ObSSGCTabletType::LOB_META, ObSSGCTabletType::LOB_CTRL};
  for (const ObSSGCTabletType type : scan_types) {
    input.table_type_ = type;
    ASSERT_EQ(OB_ERR_UNEXPECTED,
        task.prepare_lob_tablet_scan_input_(ctx, input, meta_inputs, ctrl_inputs, checkpoint));
  }
}

TEST_F(TestSSOuttableLobUserGCTask, prepare_failure_discards_outputs)
{
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSOuttableGCTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ObSSGCTabletWithMetaScnArray scan_inputs;
  ObSSGCTabletWithMetaScnArray ctrl_scan_inputs;
  ASSERT_EQ(OB_SUCCESS, ctrl_scan_inputs.push_back(ObSSGCTabletWithMetaScn(
      ObSSGCTablet(ObLSID(1), ObTabletID(200002), SCN::base_scn()), SCN::base_scn(),
      ObSSGCTabletType::UNKNOWN)));
  ASSERT_EQ(OB_SUCCESS, scan_inputs.assign(param.tablet_with_meta_scn_arr_));
  SCN min_outtable_data_scn;
  ASSERT_EQ(OB_SUCCESS, min_outtable_data_scn.convert_for_tx(100));
  // 轻量夹具没有调度器和共享元数据服务，准备失败不能留下可供后续使用的输入。
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      task.prepare_scan_inputs_(*dag.get_ctx(), scan_inputs, ctrl_scan_inputs, min_outtable_data_scn));
  ASSERT_TRUE(scan_inputs.empty());
  ASSERT_TRUE(ctrl_scan_inputs.empty());
  ASSERT_TRUE(min_outtable_data_scn.is_max());
}

TEST_F(TestSSOuttableLobUserGCTask, default_max_does_not_skip_preparation)
{
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSOuttableGCTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_TRUE(dag.get_ctx()->min_outtable_data_scn_.is_max());
  ASSERT_EQ(OB_SUCCESS, dag.get_ctx()->lob_ctrl_scan_inputs_.push_back(ObSSGCTabletWithMetaScn(
      ObSSGCTablet(ObLSID(1), ObTabletID(200002), SCN::base_scn()), SCN::base_scn(),
      ObSSGCTabletType::UNKNOWN)));
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
  ASSERT_GE(dag.get_ctx()->lob_cost_stat_.get_stage_cost_us(ObSSLobGCCostStat::PREPARE), 0);
  ASSERT_EQ(-1, dag.get_ctx()->lob_cost_stat_.get_stage_cost_us(ObSSLobGCCostStat::META_SCAN));
  ASSERT_TRUE(dag.get_ctx()->lob_ctrl_scan_inputs_.empty());
  ASSERT_TRUE(dag.get_ctx()->min_outtable_data_scn_.is_max());
  ASSERT_EQ(OB_SUCCESS, dag.get_ctx()->min_outtable_data_scn_.convert_for_tx(100));
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
  ASSERT_TRUE(dag.get_ctx()->min_outtable_data_scn_.is_max());
  ASSERT_TRUE(dag.get_ctx()->is_valid());
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_scan_uses_actual_partition_count)
{
  ObSSOuttableGCTask task;
  schema::ObTableSchema table_schema;
  ObTabletIDArray scan_inputs;
  const int64_t partition_counts[] = {16, 128, 7};
  for (int64_t count : partition_counts) {
    ASSERT_EQ(OB_SUCCESS, init_ctrl_schema(table_schema, count));
    ASSERT_EQ(OB_SUCCESS, task.collect_lob_ctrl_scan_inputs_(table_schema, scan_inputs));
    ASSERT_EQ(count, scan_inputs.count());
    for (int64_t i = 0; i < count; ++i) {
      ASSERT_EQ(ObTabletID(200001 + i), scan_inputs.at(i));
    }
  }
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_scan_inputs_are_copied_and_not_duplicated)
{
  ObSSOuttableGCTask task;
  ObTabletIDArray scan_inputs;
  {
    schema::ObTableSchema table_schema;
    ASSERT_EQ(OB_SUCCESS, init_ctrl_schema(table_schema, 3));
    ASSERT_EQ(OB_SUCCESS, scan_inputs.push_back(ObTabletID(300001)));
    ASSERT_EQ(OB_SUCCESS, task.collect_lob_ctrl_scan_inputs_(table_schema, scan_inputs));
    ASSERT_EQ(OB_SUCCESS, task.collect_lob_ctrl_scan_inputs_(table_schema, scan_inputs));
    ASSERT_EQ(3, scan_inputs.count());
    table_schema.get_part_array()[0]->set_tablet_id(ObTabletID(400001));
  }
  ASSERT_EQ(3, scan_inputs.count());
  for (int64_t i = 0; i < scan_inputs.count(); ++i) {
    ASSERT_EQ(ObTabletID(200001 + i), scan_inputs.at(i));
  }
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_scan_rejects_index_and_other_tables)
{
  ObSSOuttableGCTask task;
  schema::ObTableSchema table_schema;
  ObTabletIDArray scan_inputs;
  const schema::ObTableType rejected_types[] = {schema::USER_INDEX, schema::USER_TABLE, schema::AUX_LOB_META};
  for (schema::ObTableType type : rejected_types) {
    ASSERT_EQ(OB_SUCCESS, init_ctrl_schema(table_schema, 2));
    table_schema.set_table_type(type);
    ASSERT_EQ(OB_SUCCESS, scan_inputs.push_back(ObTabletID(300001)));
    ASSERT_EQ(OB_SCHEMA_ERROR, task.collect_lob_ctrl_scan_inputs_(table_schema, scan_inputs));
    ASSERT_TRUE(scan_inputs.empty());
  }
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_scan_rejects_invalid_tablet_and_incomplete_schema)
{
  ObSSOuttableGCTask task;
  schema::ObTableSchema table_schema;
  ObTabletIDArray scan_inputs;
  ASSERT_EQ(OB_SUCCESS, init_ctrl_schema(table_schema, 2));
  table_schema.get_part_array()[1]->set_tablet_id(ObTabletID());
  ASSERT_EQ(OB_SCHEMA_ERROR, task.collect_lob_ctrl_scan_inputs_(table_schema, scan_inputs));
  ASSERT_TRUE(scan_inputs.empty());
  ASSERT_EQ(OB_SUCCESS, init_ctrl_schema(table_schema, 2));
  table_schema.set_part_num(3);
  ASSERT_EQ(OB_SUCCESS, scan_inputs.push_back(ObTabletID(300001)));
  ASSERT_EQ(OB_SCHEMA_ERROR, task.collect_lob_ctrl_scan_inputs_(table_schema, scan_inputs));
  ASSERT_TRUE(scan_inputs.empty());
  ASSERT_EQ(OB_SUCCESS, init_ctrl_schema(table_schema, 0));
  ASSERT_EQ(OB_SUCCESS, scan_inputs.push_back(ObTabletID(300001)));
  ASSERT_EQ(OB_SCHEMA_ERROR, task.collect_lob_ctrl_scan_inputs_(table_schema, scan_inputs));
  ASSERT_TRUE(scan_inputs.empty());
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_scan_rejects_wrong_partition_level)
{
  ObSSOuttableGCTask task;
  schema::ObTableSchema table_schema;
  ObTabletIDArray scan_inputs;
  const schema::ObPartitionLevel rejected_levels[] = {schema::PARTITION_LEVEL_ZERO, schema::PARTITION_LEVEL_TWO};
  for (schema::ObPartitionLevel level : rejected_levels) {
    ASSERT_EQ(OB_SUCCESS, init_ctrl_schema(table_schema, 2));
    table_schema.set_part_level(level);
    ASSERT_EQ(OB_SUCCESS, scan_inputs.push_back(ObTabletID(300001)));
    ASSERT_EQ(OB_SCHEMA_ERROR, task.collect_lob_ctrl_scan_inputs_(table_schema, scan_inputs));
    ASSERT_TRUE(scan_inputs.empty());
  }
}

TEST_F(TestSSOuttableLobUserGCTask, non_primary_ctrl_scan_skips_schema_access)
{
  ObTabletIDArray scan_inputs;
  const ObTenantRole::Role roles[] = {ObTenantRole::STANDBY_TENANT, ObTenantRole::RESTORE_TENANT};
  for (ObTenantRole::Role role : roles) {
    ASSERT_EQ(OB_SUCCESS, scan_inputs.push_back(ObTabletID(300001)));
    ASSERT_EQ(OB_SUCCESS, prepare_ctrl_inputs_for_role(role, scan_inputs));
    ASSERT_TRUE(scan_inputs.empty());
  }
}

TEST_F(TestSSOuttableLobUserGCTask, unknown_role_does_not_prepare_ctrl_scan)
{
  ObTabletIDArray scan_inputs;
  ASSERT_EQ(OB_SUCCESS, scan_inputs.push_back(ObTabletID(300001)));
  ASSERT_EQ(OB_NEED_WAIT, prepare_ctrl_inputs_for_role(ObTenantRole::INVALID_TENANT, scan_inputs));
  ASSERT_TRUE(scan_inputs.empty());
}

TEST_F(TestSSOuttableLobUserGCTask, primary_ctrl_scan_requires_schema_service)
{
  ObTabletIDArray scan_inputs;
  ASSERT_EQ(OB_SUCCESS, scan_inputs.push_back(ObTabletID(300001)));
  ASSERT_EQ(OB_ERR_UNEXPECTED, prepare_ctrl_inputs_for_role(ObTenantRole::PRIMARY_TENANT, scan_inputs));
  ASSERT_TRUE(scan_inputs.empty());
  ObSSOuttableGCTask task;
  const uint64_t tenant_ids[] = {OB_SYS_TENANT_ID, gen_meta_tenant_id(1002)};
  for (uint64_t tenant_id : tenant_ids) {
    ASSERT_EQ(OB_SUCCESS, scan_inputs.push_back(ObTabletID(300001)));
    ASSERT_EQ(OB_ERR_UNEXPECTED, task.prepare_lob_ctrl_scan_inputs_(tenant_id, scan_inputs));
    ASSERT_TRUE(scan_inputs.empty());
  }
}

TEST_F(TestSSOuttableLobUserGCTask, scan_task_initialization)
{
  ObSSLobMetaGCScanTask task;
  ObITask *next_task = &task;
  ASSERT_EQ(OB_NOT_INIT, task.process());
  ASSERT_EQ(OB_NOT_INIT, task.generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(-1, 1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(1, 1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(2, 1));
  ASSERT_EQ(OB_SUCCESS, task.init(0, 2));
  ASSERT_EQ(OB_INIT_TWICE, task.init(2, 4));
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
}

TEST_F(TestSSOuttableLobUserGCTask, first_scan_task_skips_unusable_inputs)
{
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSOuttableGCTask task;
  ObITask *next_task = &task;
  ASSERT_EQ(OB_NOT_INIT, task.post_generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ASSERT_EQ(OB_ITER_END, task.post_generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
  ASSERT_EQ(0, dag.get_task_list_count());
  ASSERT_EQ(OB_SUCCESS, init_scan_inputs(*dag.get_ctx(), 1));
  ASSERT_EQ(OB_SUCCESS, init_ctrl_scan_inputs(*dag.get_ctx(), 1));
  dag.get_ctx()->min_outtable_data_scn_.set_min();
  ASSERT_EQ(OB_ITER_END, task.post_generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
  ASSERT_EQ(0, dag.get_task_list_count());
}

TEST_F(TestSSOuttableLobUserGCTask, scan_tasks_use_disjoint_batches)
{
  ObArenaAllocator allocator;
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  param.batch_size_ = 2;
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, init_scan_inputs(*dag.get_ctx(), 5));
  ObSSOuttableGCTask task;
  ASSERT_EQ(OB_SUCCESS, task.init());
  task.set_dag(dag);
  ObITask *next_task = nullptr;
  ObSSMarkSweepMergeCompleteTask *merge_complete_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, task.create_lob_meta_tasks_(
      dag, *dag.get_ctx(), next_task, merge_complete_task));
  ObSSLobMetaGCScanTask *first = static_cast<ObSSLobMetaGCScanTask *>(next_task);
  ASSERT_NE(nullptr, first);
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*first));
  ASSERT_EQ(0, first->start_tablet_idx_);
  ASSERT_EQ(2, first->end_tablet_idx_);
  ASSERT_EQ(OB_SUCCESS, first->generate_next_task(next_task));
  ObSSLobMetaGCScanTask *second = static_cast<ObSSLobMetaGCScanTask *>(next_task);
  ASSERT_NE(nullptr, second);
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*second));
  ASSERT_EQ(2, second->start_tablet_idx_);
  ASSERT_EQ(4, second->end_tablet_idx_);
  ASSERT_EQ(OB_SUCCESS, second->generate_next_task(next_task));
  ObSSLobMetaGCScanTask *last = static_cast<ObSSLobMetaGCScanTask *>(next_task);
  ASSERT_NE(nullptr, last);
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*last));
  ASSERT_EQ(4, last->start_tablet_idx_);
  ASSERT_EQ(5, last->end_tablet_idx_);
  ASSERT_EQ(OB_ITER_END, last->generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
}

TEST_F(TestSSOuttableLobUserGCTask, scan_batches_inherit_system_gc_dependency)
{
  ObArenaAllocator allocator;
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  param.batch_size_ = 2;
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, init_scan_inputs(*dag.get_ctx(), 3));
  ObSSOuttableGCTask *task = nullptr;
  ObSSMarkSweepSortTask *sort_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
  ASSERT_EQ(OB_SUCCESS, task->init());
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(sort_task));
  ASSERT_EQ(OB_SUCCESS, sort_task->init(0, 1));
  ASSERT_EQ(OB_SUCCESS, task->add_child(*sort_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*sort_task));
  ASSERT_EQ(1, sort_task->get_indegree());
  ObITask *next_task = nullptr;
  ObSSMarkSweepMergeCompleteTask *merge_complete_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, task->create_lob_meta_tasks_(
      dag, *dag.get_ctx(), next_task, merge_complete_task));
  ASSERT_EQ(OB_SUCCESS, task->copy_children_to(*next_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*next_task));
  ASSERT_EQ(5, dag.get_task_list_count());
  ASSERT_EQ(3, sort_task->get_indegree());
}

TEST_F(TestSSOuttableLobUserGCTask, next_scan_batch_inherits_existing_dependency)
{
  ObArenaAllocator allocator;
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  param.batch_size_ = 2;
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, init_scan_inputs(*dag.get_ctx(), 3));
  ObSSOuttableGCTask *task = nullptr;
  ObSSMarkSweepSortTask *sort_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(task));
  ASSERT_EQ(OB_SUCCESS, task->init());
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(sort_task));
  ASSERT_EQ(OB_SUCCESS, sort_task->init(0, 1));
  ASSERT_EQ(OB_SUCCESS, task->add_child(*sort_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*sort_task));
  ObITask *next_task = nullptr;
  ObSSMarkSweepMergeCompleteTask *merge_complete_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, task->create_lob_meta_tasks_(
      dag, *dag.get_ctx(), next_task, merge_complete_task));
  ObSSLobMetaGCScanTask *first = static_cast<ObSSLobMetaGCScanTask *>(next_task);
  ASSERT_EQ(OB_SUCCESS, task->copy_children_to(*first));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*first));
  ASSERT_EQ(5, dag.get_task_list_count());
  ASSERT_EQ(3, sort_task->get_indegree());
  ASSERT_EQ(OB_SUCCESS, first->ObITask::generate_next_task());
  ASSERT_EQ(6, dag.get_task_list_count());
  ASSERT_EQ(4, sort_task->get_indegree());
}

TEST_F(TestSSOuttableLobUserGCTask, scan_range_is_checked_before_file_operations)
{
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, init_scan_inputs(*dag.get_ctx(), 1));
  ObSSLobMetaGCScanTask task;
  ASSERT_EQ(OB_SUCCESS, task.init(0, 2));
  task.set_dag(dag);
  ObITask *next_task = &task;
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
  ASSERT_GE(dag.get_ctx()->lob_cost_stat_.get_stage_cost_us(ObSSLobGCCostStat::META_SCAN), 0);
  ASSERT_TRUE(dag.get_ctx()->is_valid());

  ObSSLobCtrlGCScanTask ctrl_task;
  ASSERT_EQ(OB_SUCCESS, ctrl_task.init(0, 2));
  ctrl_task.set_dag(dag);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ctrl_task.process());
  ASSERT_GE(dag.get_ctx()->lob_cost_stat_.get_stage_cost_us(ObSSLobGCCostStat::CTRL_SCAN), 0);
  dag.get_ctx()->reset();
  ASSERT_EQ(-1, dag.get_ctx()->lob_cost_stat_.get_total_cost_us());
}

TEST_F(TestSSOuttableLobUserGCTask, missing_meta_service_is_not_an_empty_keep_set)
{
  ObSSLobMetaGCScanTask task;
  ObMarkSweepSorter sorter;
  ObSSMarkSweepDiagMgr diag_mgr;
  const ObSSGCTabletWithMetaScn scan_input(
      ObSSGCTablet(ObLSID(1), ObTabletID(200001), SCN::base_scn()), SCN::base_scn(),
      ObSSGCTabletType::UNKNOWN);
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.scan_lob_meta_tablet_(scan_input, sorter, diag_mgr));
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      task.scan_lob_meta_tablet_(ObSSGCTabletWithMetaScn(), sorter, diag_mgr));
}

TEST_F(TestSSOuttableLobUserGCTask, meta_scan_skips_non_outtable_rows)
{
  ObArenaAllocator allocator;
  blocksstable::ObDatumRow row;
  blocksstable::MacroBlockId macro_id;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, 2));
  row.storage_datums_[0].set_null();
  row.storage_datums_[1].set_null();
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator, macro_id));
  ASSERT_FALSE(macro_id.is_valid());

  ObLobMetaExtraInfo extra_info;
  ObString encoded_info;
  extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
  extra_info.lob_chunk_type_.set_in_table();
  ASSERT_EQ(OB_SUCCESS, extra_info.serialize(allocator, encoded_info));
  row.storage_datums_[0].set_string(encoded_info);
  row.storage_datums_[1].set_null();
  ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator, macro_id));
  ASSERT_FALSE(macro_id.is_valid());

  extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  extra_info.lob_chunk_type_.set_out_table();
  extra_info.chunk_count_ = 1;
  extra_info.compressed_len_ = 1;
  ASSERT_EQ(OB_SUCCESS, extra_info.serialize(allocator, encoded_info));
  row.storage_datums_[0].set_string(encoded_info);
  ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator, macro_id));
  ASSERT_FALSE(macro_id.is_valid());

  ObExternalBlobInfo external_info;
  external_info.url_ = ObString::make_string("s3://test/external_lob");
  external_info.length_ = 16;
  external_info.kind_ = ObExternalBlobKind::REFERENCE;
  extra_info.reset();
  extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  ASSERT_EQ(OB_SUCCESS, extra_info.set_external_lob_info(allocator, external_info));
  ASSERT_EQ(OB_SUCCESS, extra_info.serialize(allocator, encoded_info));
  ObString external_id;
  ASSERT_EQ(OB_SUCCESS,
      ObExternalLobMacroIdPrefix::serialize(allocator, 200001, 10, external_id));
  ASSERT_EQ(17, external_id.length());
  row.storage_datums_[0].set_string(encoded_info);
  row.storage_datums_[1].set_string(external_id);
  ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator, macro_id));
  ASSERT_FALSE(macro_id.is_valid());
}

TEST_F(TestSSOuttableLobUserGCTask, meta_scan_preserves_all_physical_outtable_references)
{
  ObArenaAllocator allocator;
  blocksstable::ObDatumRow row;
  blocksstable::MacroBlockId expected;
  blocksstable::MacroBlockId actual;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, 2));
  expected.set_id_mode(static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_SHARE));
  expected.set_storage_object_type(static_cast<uint64_t>(
      blocksstable::ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO));
  expected.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
  expected.set_third_id(1001);
  expected.set_fourth_id(2002);
  ObLobMetaExtraInfo extra_info;
  ObString encoded_info;
  extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
  extra_info.lob_chunk_type_.set_out_table();
  extra_info.has_crc_ = true;
  extra_info.compressed_len_ = 1;
  ASSERT_EQ(OB_SUCCESS, extra_info.serialize(allocator, encoded_info));
  row.storage_datums_[0].set_string(encoded_info);
  const int64_t versions[] = {100, 200, 100};
  for (int64_t i = 0; i < ARRAYSIZEOF(versions); ++i) {
    // Distinct and repeated physical references must all reach the retained-set sorter.
    expected.set_second_id(versions[i]);
    ObString encoded_id;
    ASSERT_EQ(OB_SUCCESS, ObLobOutTableIO::serialize_macro_id(allocator, expected, encoded_id));
    row.storage_datums_[1].set_string(encoded_id);
    ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator, actual));
    ASSERT_EQ(expected, actual);
  }
}

TEST_F(TestSSOuttableLobUserGCTask, meta_scan_rejects_missing_or_invalid_outtable_reference)
{
  ObArenaAllocator allocator;
  blocksstable::ObDatumRow row;
  blocksstable::MacroBlockId macro_id;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator, 2));
  ObLobMetaExtraInfo extra_info;
  ObString encoded_info;
  extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
  extra_info.lob_chunk_type_.set_out_table();
  extra_info.has_crc_ = true;
  extra_info.compressed_len_ = 1;
  ASSERT_EQ(OB_SUCCESS, extra_info.serialize(allocator, encoded_info));
  row.storage_datums_[0].set_string(encoded_info);
  row.storage_datums_[1].set_null();
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator, macro_id));
  row.storage_datums_[1].set_string("", 0);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator, macro_id));
  row.storage_datums_[1].set_string("bad", 3);
  ASSERT_NE(OB_SUCCESS, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator, macro_id));

  ObString external_id;
  ASSERT_EQ(OB_SUCCESS,
      ObExternalLobMacroIdPrefix::serialize(allocator, 200001, 10, external_id));
  row.storage_datums_[1].set_string(external_id);
  ASSERT_NE(OB_SUCCESS, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator, macro_id));
  row.storage_datums_[0].set_null();
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator, macro_id));
  row.storage_datums_[0].set_string("invalid json", 12);
  ASSERT_NE(OB_SUCCESS, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator, macro_id));
  row.count_ = 1;
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator, macro_id));
}

TEST_F(TestSSOuttableLobUserGCTask, merge_start_task_initialization)
{
  ObMarkSweepPipeline pipeline;
  ObSSMarkSweepMergeCompleteTask complete;
  ObSSMarkSweepMergeStartTask task;
  ASSERT_EQ(OB_NOT_INIT, task.process());
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(nullptr, &pipeline));
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(&complete, nullptr));
  ASSERT_EQ(OB_SUCCESS, task.init(&complete, &pipeline));
  ASSERT_EQ(&pipeline, task.pipeline_);
  ASSERT_EQ(OB_INIT_TWICE, task.init(&complete, &pipeline));
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
}

TEST_F(TestSSOuttableLobUserGCTask, merge_pass_task_initialization)
{
  ObMarkSweepPipeline pipeline;
  ObSSMarkSweepMergeCompleteTask complete;
  ObSSMarkSweepMergePassCtrlTask task;
  ASSERT_EQ(OB_NOT_INIT, task.process());
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(ObMarkSweepGroupType::KEPT, 0, &complete, nullptr));
  ASSERT_EQ(OB_SUCCESS, task.init(ObMarkSweepGroupType::KEPT, 0, &complete, &pipeline));
  ASSERT_EQ(&pipeline, task.pipeline_);
  ASSERT_EQ(OB_INIT_TWICE, task.init(ObMarkSweepGroupType::KEPT, 0, &complete, &pipeline));
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
}

TEST_F(TestSSOuttableLobUserGCTask, merge_batch_task_initialization)
{
  ObMarkSweepPipeline pipeline;
  ObSSMarkSweepMergeBatchTask task;
  ASSERT_EQ(OB_NOT_INIT, task.process());
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(ObMarkSweepGroupType::KEPT, 0, 2, 0, nullptr));
  ASSERT_EQ(OB_SUCCESS, task.init(ObMarkSweepGroupType::KEPT, 0, 2, 0, &pipeline));
  ASSERT_EQ(&pipeline, task.pipeline_);
  ASSERT_EQ(OB_INIT_TWICE, task.init(ObMarkSweepGroupType::KEPT, 0, 2, 0, &pipeline));
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
}

TEST_F(TestSSOuttableLobUserGCTask, merge_complete_task_initialization)
{
  ObMarkSweepPipeline pipeline;
  ObSSMarkSweepMergeCompleteTask task;
  ASSERT_EQ(OB_NOT_INIT, task.process());
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(nullptr));
  ASSERT_EQ(OB_SUCCESS, task.init(&pipeline));
  ASSERT_EQ(&pipeline, task.pipeline_);
  ASSERT_EQ(OB_INIT_TWICE, task.init(&pipeline));
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
}

TEST_F(TestSSOuttableLobUserGCTask, merge_start_propagates_kept_pipeline_to_pass)
{
  ObArenaAllocator allocator;
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObMarkSweepPipeline *pipeline = &dag.get_ctx()->lob_meta_pipeline_;
  ObSSMarkSweepMergeStartTask *start_task = nullptr;
  ObSSMarkSweepMergeCompleteTask *complete_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(complete_task));
  ASSERT_EQ(OB_SUCCESS, complete_task->init(pipeline));
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(start_task));
  ASSERT_EQ(OB_SUCCESS, start_task->init(complete_task, pipeline));
  ASSERT_EQ(OB_SUCCESS, start_task->add_child(*complete_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*start_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*complete_task));
  dag.set_dag_status(ObIDag::DAG_STATUS_NODE_RUNNING);
  ObITask *ready_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(start_task, ready_task);
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_SUCCESS, start_task->create_pass_ctrl_task_(ObMarkSweepGroupType::KEPT));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(ObITask::TASK_TYPE_MARK_SWEEP_MERGE_PASS_CTRL, ready_task->get_type());
  ObSSMarkSweepMergePassCtrlTask *pass = static_cast<ObSSMarkSweepMergePassCtrlTask *>(ready_task);
  ASSERT_EQ(pipeline, pass->pipeline_);
  ASSERT_EQ(ObMarkSweepGroupType::KEPT, pass->get_group());
  ASSERT_EQ(0, pass->pass_idx_);
  ASSERT_EQ(complete_task, pass->merge_complete_task_);
}

TEST_F(TestSSOuttableLobUserGCTask, next_merge_batch_preserves_kept_pipeline)
{
  ObArenaAllocator allocator;
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObMarkSweepPipeline *pipeline = &dag.get_ctx()->lob_meta_pipeline_;
  ObSSMarkSweepMergeBatchTask *first_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(first_task));
  ASSERT_EQ(OB_SUCCESS, first_task->init(ObMarkSweepGroupType::KEPT, 0, 2, 3, pipeline));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*first_task));
  ObITask *next_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, first_task->generate_next_task(next_task));
  ASSERT_NE(nullptr, next_task);
  ObSSMarkSweepMergeBatchTask *next_batch = static_cast<ObSSMarkSweepMergeBatchTask *>(next_task);
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*next_batch));
  ASSERT_EQ(pipeline, next_batch->pipeline_);
  ASSERT_EQ(ObMarkSweepGroupType::KEPT, next_batch->group_);
  ASSERT_EQ(1, next_batch->batch_idx_);
  ASSERT_EQ(2, next_batch->total_batch_count_);
  ASSERT_EQ(3, next_batch->pass_idx_);
  ASSERT_EQ(OB_ITER_END, next_batch->generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
}

TEST_F(TestSSOuttableLobUserGCTask, dag_keeps_user_gc_as_only_lob_entry)
{
  ObArenaAllocator allocator;
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ASSERT_EQ(6, dag.get_task_list_count());
}

TEST_F(TestSSOuttableLobUserGCTask, dag_skips_user_gc_before_min_cluster_version)
{
  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_6_1_0);
  ObArenaAllocator allocator;
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.create_first_task());
  ASSERT_EQ(5, dag.get_task_list_count());
  ASSERT_EQ(-1, dag.get_ctx()->lob_cost_stat_.get_total_cost_us());
}

TEST_F(TestSSOuttableLobUserGCTask, merge_waits_for_all_scans_and_blocks_system_sort)
{
  ObArenaAllocator allocator;
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  param.batch_size_ = 2;
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, init_scan_inputs(*dag.get_ctx(), 3));
  ObSSOuttableGCTask *user_task = nullptr;
  ObSSMarkSweepSortTask *sort_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(user_task));
  ASSERT_EQ(OB_SUCCESS, user_task->init());
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(sort_task));
  ASSERT_EQ(OB_SUCCESS, sort_task->init(0, 1));
  ASSERT_EQ(OB_SUCCESS, user_task->add_child(*sort_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*user_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*sort_task));
  dag.set_dag_status(ObIDag::DAG_STATUS_NODE_RUNNING);

  ObITask *ready_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(user_task, ready_task);
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ObITask *generated_scan_task = nullptr;
  ObSSMarkSweepMergeCompleteTask *lob_merge_complete_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, user_task->create_lob_meta_tasks_(
      dag, *dag.get_ctx(), generated_scan_task, lob_merge_complete_task));
  ASSERT_EQ(OB_SUCCESS, user_task->copy_children_to(*generated_scan_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*generated_scan_task));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
  ASSERT_EQ(nullptr, ready_task);

  ObITask *first_scan = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(first_scan));
  ASSERT_EQ(ObITask::TASK_TYPE_SS_LOB_META_GC_SCAN, first_scan->get_type());
  first_scan->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_SUCCESS, first_scan->generate_next_task());
  ObITask *last_scan = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(last_scan));
  ASSERT_EQ(ObITask::TASK_TYPE_SS_LOB_META_GC_SCAN, last_scan->get_type());
  last_scan->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_SUCCESS, last_scan->generate_next_task());
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));

  // 只结束部分扫描时，归并和系统排序均不能就绪。
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(first_scan));
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(last_scan));
  ObITask *merge_start_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(merge_start_task));
  ASSERT_EQ(ObITask::TASK_TYPE_MARK_SWEEP_MERGE_START, merge_start_task->get_type());
  ASSERT_EQ(&dag.get_ctx()->lob_meta_pipeline_,
      static_cast<ObSSMarkSweepMergeStartTask *>(merge_start_task)->pipeline_);
  merge_start_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));

  ASSERT_EQ(OB_SUCCESS, dag.finish_task(merge_start_task));
  ObITask *merge_complete_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(merge_complete_task));
  ASSERT_EQ(ObITask::TASK_TYPE_MARK_SWEEP_MERGE_COMPLETE, merge_complete_task->get_type());
  ASSERT_EQ(&dag.get_ctx()->lob_meta_pipeline_,
      static_cast<ObSSMarkSweepMergeCompleteTask *>(merge_complete_task)->pipeline_);
  merge_complete_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(merge_complete_task));
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(sort_task, ready_task);
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
  ASSERT_EQ(0, dag.get_task_list_count());
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_scan_task_initialization)
{
  ObSSLobCtrlGCScanTask task;
  ObITask *next_task = &task;
  ASSERT_EQ(OB_NOT_INIT, task.process());
  ASSERT_EQ(OB_NOT_INIT, task.generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(-1, 1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(1, 1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(2, 1));
  ASSERT_EQ(OB_SUCCESS, task.init(0, 2));
  ASSERT_EQ(OB_INIT_TWICE, task.init(2, 4));
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
  next_task = &task;
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_scan_columns_follow_frozen_storage_schema)
{
  ObArenaAllocator allocator;
  schema::ObTableSchema table_schema;
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  ASSERT_EQ(OB_SUCCESS, all_outtable_lob_control_schema(table_schema));
  table_schema.set_table_id(500001);
  table_schema.set_schema_version(100);
  for (const bool simplified : {false, true}) {
    ObStorageSchema storage_schema;
    ASSERT_EQ(OB_SUCCESS,
        storage_schema.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL, simplified));
    ASSERT_EQ(simplified, storage_schema.is_column_info_simplified());
    ObSEArray<schema::ObColDesc, 4> physical_columns;
    ASSERT_EQ(OB_SUCCESS, storage_schema.get_mulit_version_rowkey_column_ids(physical_columns));
    ASSERT_EQ(4, physical_columns.count());
    ASSERT_EQ(OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID, physical_columns.at(3).col_id_);
    ObSEArray<schema::ObColDesc, 3> columns;
    ASSERT_EQ(OB_SUCCESS, columns.push_back(physical_columns.at(3)));
    ASSERT_EQ(OB_SUCCESS, ObSSLobGCScanUtil::prepare_ctrl_scan_columns(storage_schema, columns));
    ASSERT_EQ(3, columns.count());
    ASSERT_EQ(OB_APP_MIN_COLUMN_ID, columns.at(0).col_id_);
    ASSERT_EQ(OB_APP_MIN_COLUMN_ID + 1, columns.at(1).col_id_);
    ASSERT_EQ(OB_HIDDEN_TRANS_VERSION_COLUMN_ID, columns.at(2).col_id_);
    for (int64_t i = 0; i < columns.count(); ++i) {
      ASSERT_EQ(physical_columns.at(i).col_id_, columns.at(i).col_id_);
      ASSERT_EQ(physical_columns.at(i).col_type_, columns.at(i).col_type_);
      ASSERT_EQ(physical_columns.at(i).col_order_, columns.at(i).col_order_);
      ASSERT_NE(OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID, columns.at(i).col_id_);
    }
  }
}

TEST_F(TestSSOuttableLobUserGCTask, meta_scan_columns_use_fixed_layout)
{
  ObArenaAllocator allocator;
  schema::ObTableSchema table_schema;
  table_schema.set_tenant_id(OB_SYS_TENANT_ID);
  table_schema.set_database_id(OB_SYS_DATABASE_ID);
  ASSERT_EQ(OB_SUCCESS, ObInnerTableSchema::all_tenant_profile_aux_lob_meta_schema(table_schema));
  table_schema.set_table_id(500001);
  table_schema.set_schema_version(100);
  ObSEArray<schema::ObColDesc, 2> columns;
  ObObjMeta column_type;
  column_type.set_varbinary();
  column_type.set_collation_type(CS_TYPE_BINARY);
  // Column preparation needs no schema and replaces any previously requested columns.
  schema::ObColDesc unused_column;
  unused_column.col_id_ = OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID;
  ASSERT_EQ(OB_SUCCESS, columns.push_back(unused_column));
  for (const bool simplified : {false, true}) {
    ObStorageSchema storage_schema;
    ASSERT_EQ(OB_SUCCESS,
        storage_schema.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL, simplified));
    ASSERT_EQ(simplified, storage_schema.is_column_info_simplified());
    ASSERT_EQ(OB_SUCCESS, ObSSLobGCScanUtil::prepare_meta_scan_columns(columns));
    ASSERT_EQ(2, columns.count());
    ASSERT_EQ(OB_APP_MIN_COLUMN_ID + ObLobMetaUtil::LOB_INFO_COL_ID, columns.at(0).col_id_);
    ASSERT_EQ(OB_APP_MIN_COLUMN_ID + ObLobMetaUtil::LOB_BLOCK_ID_COL_ID, columns.at(1).col_id_);
    ASSERT_EQ(column_type, columns.at(0).col_type_);
    ASSERT_EQ(column_type, columns.at(1).col_type_);
    if (!simplified) {
      ObSEArray<schema::ObColDesc, 16> physical_columns;
      ASSERT_EQ(OB_SUCCESS, storage_schema.get_multi_version_column_descs(physical_columns));
      int64_t matched_count = 0;
      for (int64_t i = 0; i < physical_columns.count(); ++i) {
        const schema::ObColDesc &column = physical_columns.at(i);
        if (column.col_id_ == columns.at(0).col_id_ || column.col_id_ == columns.at(1).col_id_) {
          ASSERT_LT(matched_count, columns.count());
          ASSERT_EQ(column.col_id_, columns.at(matched_count).col_id_);
          ASSERT_EQ(column.col_type_, columns.at(matched_count).col_type_);
          ASSERT_EQ(column.col_order_, columns.at(matched_count).col_order_);
          ++matched_count;
        }
      }
      ASSERT_EQ(2, matched_count);
    }
  }
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_merge_task_initialization)
{
  ObSSLobCtrlGCMergeTask task;
  ASSERT_EQ(OB_NOT_INIT, task.process());
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(-1, 1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(0, 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.init(1, 1));
  ASSERT_EQ(OB_SUCCESS, task.init(0, 1));
  ASSERT_EQ(OB_INIT_TWICE, task.init(0, 1));
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
  ObSSLobCtrlGCMergePassTask pass_task;
  ASSERT_EQ(OB_NOT_INIT, pass_task.process());
  ASSERT_EQ(OB_INVALID_ARGUMENT, pass_task.init(-1));
  ASSERT_EQ(OB_SUCCESS, pass_task.init(0));
  ASSERT_EQ(OB_INIT_TWICE, pass_task.init(1));
  ASSERT_EQ(OB_ERR_UNEXPECTED, pass_task.process());
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_diff_task_initialization)
{
  ObSSLobCtrlGCDiffTask task;
  ASSERT_EQ(OB_NOT_INIT, task.process());
  ASSERT_EQ(OB_SUCCESS, task.init());
  ASSERT_EQ(OB_INIT_TWICE, task.init());
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_delete_task_initialization)
{
  ObSSLobCtrlGCDeleteTask task;
  ASSERT_EQ(OB_NOT_INIT, task.process());
  ASSERT_EQ(ObITask::TASK_TYPE_SS_LOB_CTRL_GC_DELETE, task.get_type());
  ASSERT_EQ(OB_SUCCESS, task.init());
  ASSERT_EQ(OB_INIT_TWICE, task.init());
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
}

TEST_F(TestSSOuttableLobUserGCTask, merged_file_accessor_rejects_unavailable_input)
{
  ObMarkSweepPipeline pipeline;
  ObSortedFileInfo file;
  ASSERT_EQ(OB_NOT_INIT, pipeline.get_merged_file(ObMarkSweepGroupType::KEPT, file));
  ASSERT_FALSE(file.is_valid());
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_candidate_compare_uses_macro_id_only)
{
  ObSSOuttableLobCtrlCandidate left;
  left.macro_id_.set_block_index(1);
  left.hash_ = UINT64_MAX;
  left.mark_row_scn_ = 200;
  ObSSOuttableLobCtrlCandidate right = left;
  right.macro_id_.set_block_index(2);
  right.hash_ = 0;
  ObSSLobCtrlGCScanCompare compare;
  ASSERT_TRUE(compare(&left, &right));
  ASSERT_FALSE(compare(&right, &left));
  right.hash_ = left.hash_;
  ASSERT_TRUE(compare(&left, &right));
  ASSERT_FALSE(compare(&left, &left));
  ASSERT_EQ(OB_SUCCESS, compare.result_code_);
}

TEST_F(TestSSOuttableLobUserGCTask, lob_ctrl_diag_record_round_trip)
{
  ObSEArray<ObSSOuttableLobCtrlCandidate, 2> expected_rows;
  int64_t record_size = serialization::encoded_length_i64(2);
  for (int64_t i = 0; i < 2; ++i) {
    ObSSOuttableLobCtrlCandidate row;
    row.macro_id_ = blocksstable::MacroBlockId(
        blocksstable::MacroBlockId::DEFAULT_FIRST_ID, i + 1, 0, 0, 0);
    row.hash_ = 42;
    row.mark_row_scn_ = 200 + i;
    ASSERT_TRUE(row.is_valid());
    ASSERT_EQ(OB_SUCCESS, expected_rows.push_back(row));
    record_size += row.get_serialize_size();
  }
  ObArenaAllocator allocator(ObModIds::TEST);
  char *buf = static_cast<char *>(allocator.alloc(record_size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(
      buf, record_size, pos, expected_rows.count()));
  for (int64_t i = 0; i < expected_rows.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, expected_rows.at(i).serialize(buf, record_size, pos));
  }
  ASSERT_EQ(record_size, pos);
  ObSSLobCtrlGCDiagRows decoded_rows;
  ASSERT_EQ(OB_SUCCESS, ObSSMarkSweepDiagMgr::decode_lob_ctrl_record(
      ObString(static_cast<int32_t>(pos), buf), decoded_rows));
  ASSERT_EQ(expected_rows.count(), decoded_rows.rows_.count());
  for (int64_t i = 0; i < expected_rows.count(); ++i) {
    const ObSSOuttableLobCtrlCandidate &expected = expected_rows.at(i);
    const ObSSOuttableLobCtrlCandidate &actual = decoded_rows.rows_.at(i);
    EXPECT_EQ(expected.macro_id_, actual.macro_id_);
    EXPECT_EQ(expected.hash_, actual.hash_);
    EXPECT_EQ(expected.mark_row_scn_, actual.mark_row_scn_);
  }
  ASSERT_TRUE(ObSSMarkSweepDiagMgr::is_lob_ctrl_type(
      ObSSMarkSweepDiagFileType::LOB_CTRL_CANDIDATE));
  ASSERT_TRUE(ObSSMarkSweepDiagMgr::is_lob_ctrl_type(
      ObSSMarkSweepDiagFileType::LOB_CTRL_DIFF));
  ASSERT_FALSE(ObSSMarkSweepDiagMgr::is_lob_ctrl_type(
      ObSSMarkSweepDiagFileType::LOB_META_KEPT));
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_merge_batches_run_in_parallel_and_next_pass_waits)
{
  ObArenaAllocator allocator;
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  // 只验证调度依赖，不运行文件归并；129 个占位输入对应三个互不重叠的批次。
  ASSERT_EQ(OB_SUCCESS, dag.get_ctx()->lob_ctrl_merge_inputs_.prepare_allocate(129));
  ObSSLobCtrlGCMergePassTask *pass_task = nullptr;
  ObSSLobCtrlGCDiffTask *diff_task = nullptr;
  ObSSLobCtrlGCDeleteTask *delete_task = nullptr;
  ObSSMarkSweepSortTask *sort_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(pass_task));
  ASSERT_EQ(OB_SUCCESS, pass_task->init(0));
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(diff_task));
  ASSERT_EQ(OB_SUCCESS, diff_task->init());
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(delete_task));
  ASSERT_EQ(OB_SUCCESS, delete_task->init());
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(sort_task));
  ASSERT_EQ(OB_SUCCESS, sort_task->init(0, 1));
  ASSERT_EQ(OB_SUCCESS, delete_task->add_child(*sort_task));
  ASSERT_EQ(OB_SUCCESS, diff_task->add_child(*delete_task));
  ASSERT_EQ(OB_SUCCESS, pass_task->add_child(*diff_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*pass_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*diff_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*delete_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*sort_task));
  dag.set_dag_status(ObIDag::DAG_STATUS_NODE_RUNNING);
  ObITask *ready_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(pass_task, ready_task);
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_SUCCESS, pass_task->create_batch_tasks_(*dag.get_ctx()));
  ASSERT_EQ(6, dag.get_task_list_count());
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));
  ObITask *finished_pass = pass_task;
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(finished_pass));
  ObITask *batches[3] = {};
  for (int64_t i = 0; i < ARRAYSIZEOF(batches); ++i) {
    // 不结束前一个批次，下一个仍可就绪，证明批次之间没有串行依赖。
    ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(batches[i]));
    ASSERT_EQ(ObITask::TASK_TYPE_SS_LOB_CTRL_GC_MERGE, batches[i]->get_type());
    ObSSLobCtrlGCMergeTask *batch = static_cast<ObSSLobCtrlGCMergeTask *>(batches[i]);
    ASSERT_EQ(i, batch->batch_idx_);
    ASSERT_EQ(3, batch->total_batch_count_);
    batches[i]->set_status(ObITask::TASK_STATUS_RUNNING);
    ASSERT_EQ(OB_SUCCESS, batches[i]->generate_next_task());
  }
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(batches[1]));
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(batches[2]));
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(batches[0]));
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(ObITask::TASK_TYPE_SS_LOB_CTRL_GC_MERGE_PASS, ready_task->get_type());
  ASSERT_EQ(1, static_cast<ObSSLobCtrlGCMergePassTask *>(ready_task)->pass_idx_);
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(batches[0]));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(diff_task, ready_task);
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(batches[0]));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(delete_task, ready_task);
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(batches[0]));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(sort_task, ready_task);
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
  ASSERT_EQ(0, dag.get_task_list_count());
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_scan_tasks_use_disjoint_batches)
{
  ObArenaAllocator allocator;
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  param.batch_size_ = 2;
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, init_ctrl_scan_inputs(*dag.get_ctx(), 5));
  ObSSLobCtrlGCScanTask *first = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(first));
  ASSERT_EQ(OB_SUCCESS, first->init(0, 2));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*first));
  ASSERT_EQ(0, first->start_tablet_idx_);
  ASSERT_EQ(2, first->end_tablet_idx_);
  ObITask *next_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, first->generate_next_task(next_task));
  ObSSLobCtrlGCScanTask *second = static_cast<ObSSLobCtrlGCScanTask *>(next_task);
  ASSERT_NE(nullptr, second);
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*second));
  ASSERT_EQ(2, second->start_tablet_idx_);
  ASSERT_EQ(4, second->end_tablet_idx_);
  ASSERT_EQ(OB_SUCCESS, second->generate_next_task(next_task));
  ObSSLobCtrlGCScanTask *last = static_cast<ObSSLobCtrlGCScanTask *>(next_task);
  ASSERT_NE(nullptr, last);
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*last));
  ASSERT_EQ(4, last->start_tablet_idx_);
  ASSERT_EQ(5, last->end_tablet_idx_);
  ASSERT_EQ(OB_ITER_END, last->generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_scan_range_is_checked_before_scanning)
{
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, init_ctrl_scan_inputs(*dag.get_ctx(), 1));
  ObSSLobCtrlGCScanTask task;
  ASSERT_EQ(OB_SUCCESS, task.init(0, 2));
  task.set_dag(dag);
  ObITask *next_task = &task;
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
  ASSERT_TRUE(dag.get_ctx()->is_valid());
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_scan_rejects_invalid_input_tablet)
{
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, dag.get_ctx()->lob_ctrl_scan_inputs_.push_back(ObSSGCTabletWithMetaScn()));
  ObSSLobCtrlGCScanTask task;
  ASSERT_EQ(OB_SUCCESS, task.init(0, 1));
  task.set_dag(dag);
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.process());
  ASSERT_TRUE(dag.get_ctx()->is_valid());
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_scan_rejects_invalid_version_and_missing_context)
{
  ObSSLobCtrlGCScanTask task;
  ObSSLobCtrlGCScanSorter sorter;
  ASSERT_EQ(OB_INVALID_ARGUMENT, task.scan_lob_ctrl_tablet_(ObSSGCTabletWithMetaScn(), sorter));
  const ObSSGCTabletWithMetaScn scan_input(
      ObSSGCTablet(ObLSID(1), ObTabletID(400001), SCN::base_scn()), SCN::base_scn(),
      ObSSGCTabletType::UNKNOWN);
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.scan_lob_ctrl_tablet_(scan_input, sorter));
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_scan_requires_finite_boundary_before_loading_tablet)
{
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ObSSMarkSweepCtx &ctx = *dag.get_ctx();
  ASSERT_EQ(OB_SUCCESS, init_ctrl_scan_inputs(ctx, 1));
  ObSSLobCtrlGCScanTask task;
  ASSERT_EQ(OB_SUCCESS, task.init(0, 1));
  task.set_dag(dag);
  ObSSLobCtrlGCScanSorter sorter;
  ObSSMetaService meta_service;
  ObSSDiagnoseInfoMgr diagnose_mgr;
  ObTenantBase *tenant = ObTenantEnv::get_tenant();
  ASSERT_NE(nullptr, tenant);
  ObSSMetaService *previous_meta_service = tenant->get<ObSSMetaService *>();
  ObSSDiagnoseInfoMgr *previous_diagnose_mgr = tenant->get<ObSSDiagnoseInfoMgr *>();
  DEFER(tenant->set(previous_meta_service);
        tenant->set(previous_diagnose_mgr);
        ObTenantEnv::set_tenant(tenant));
  tenant->set(&meta_service);
  tenant->set(&diagnose_mgr);
  ObTenantEnv::set_tenant(tenant);
  const ObSSGCTabletWithMetaScn &scan_input = ctx.lob_ctrl_scan_inputs_.at(0);
  // A valid boundary reaches the uninitialized service; invalid boundaries must not reach it.
  ASSERT_EQ(OB_NOT_INIT, task.scan_lob_ctrl_tablet_(scan_input, sorter));
  ctx.min_outtable_data_scn_.set_min();
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.scan_lob_ctrl_tablet_(scan_input, sorter));
  ctx.min_outtable_data_scn_.set_max();
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.scan_lob_ctrl_tablet_(scan_input, sorter));
  ctx.min_outtable_data_scn_.reset();
  ASSERT_EQ(OB_ERR_UNEXPECTED, task.scan_lob_ctrl_tablet_(scan_input, sorter));
  ASSERT_EQ(OB_SUCCESS, ctx.min_outtable_data_scn_.convert_for_tx(100));
  ASSERT_EQ(OB_NOT_INIT, task.scan_lob_ctrl_tablet_(scan_input, sorter));
}

TEST_F(TestSSOuttableLobUserGCTask, empty_ctrl_inputs_do_not_generate_outtable_gc_tasks)
{
  ObArenaAllocator allocator;
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, init_scan_inputs(*dag.get_ctx(), 1));
  ASSERT_TRUE(dag.get_ctx()->lob_ctrl_scan_inputs_.empty());
  ObSSOuttableGCTask *user_task = nullptr;
  ObSSMarkSweepSortTask *sort_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(user_task));
  ASSERT_EQ(OB_SUCCESS, user_task->init());
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(sort_task));
  ASSERT_EQ(OB_SUCCESS, sort_task->init(0, 1));
  ASSERT_EQ(OB_SUCCESS, user_task->add_child(*sort_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*user_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*sort_task));
  ObITask *next_task = nullptr;
  ASSERT_EQ(OB_ITER_END, user_task->post_generate_next_task(next_task));
  ASSERT_EQ(nullptr, next_task);
  ASSERT_EQ(2, dag.get_task_list_count());
  ASSERT_EQ(1, sort_task->get_indegree());
}

TEST_F(TestSSOuttableLobUserGCTask, max_scn_with_meta_inputs_waits_for_diff_and_blocks_system_sort)
{
  ObArenaAllocator allocator;
  ObSSMarkSweepDagParam param;
  ASSERT_EQ(OB_SUCCESS, init_param(param));
  param.batch_size_ = 2;
  ObSSMarkSweepDag dag;
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
  ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
  ASSERT_EQ(OB_SUCCESS, init_scan_inputs(*dag.get_ctx(), 3));
  ASSERT_EQ(OB_SUCCESS, init_ctrl_scan_inputs(*dag.get_ctx(), 3));
  dag.get_ctx()->min_outtable_data_scn_.set_max();
  ObSSOuttableGCTask *user_task = nullptr;
  ObSSMarkSweepSortTask *sort_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(user_task));
  ASSERT_EQ(OB_SUCCESS, user_task->init());
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(sort_task));
  ASSERT_EQ(OB_SUCCESS, sort_task->init(0, 1));
  ASSERT_EQ(OB_SUCCESS, user_task->add_child(*sort_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*user_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*sort_task));
  dag.set_dag_status(ObIDag::DAG_STATUS_NODE_RUNNING);
  ObITask *ready_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(user_task, ready_task);
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_SUCCESS, ready_task->post_generate_next_task());
  ASSERT_EQ(9, dag.get_task_list_count());
  ASSERT_EQ(4, sort_task->get_indegree());
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
  ObITask *first_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(first_meta));
  ASSERT_EQ(ObITask::TASK_TYPE_SS_LOB_META_GC_SCAN, first_meta->get_type());
  first_meta->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_SUCCESS, first_meta->generate_next_task());
  ASSERT_EQ(9, dag.get_task_list_count());
  ASSERT_EQ(4, sort_task->get_indegree());
  ObITask *last_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(last_meta));
  ASSERT_EQ(ObITask::TASK_TYPE_SS_LOB_META_GC_SCAN, last_meta->get_type());
  last_meta->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_SUCCESS, last_meta->generate_next_task());
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(first_meta));
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(last_meta));
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(ObITask::TASK_TYPE_MARK_SWEEP_MERGE_START, ready_task->get_type());
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(first_meta));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(ObITask::TASK_TYPE_MARK_SWEEP_MERGE_COMPLETE, ready_task->get_type());
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(first_meta));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
  ObITask *first_ctrl = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(first_ctrl));
  ASSERT_EQ(ObITask::TASK_TYPE_SS_LOB_CTRL_GC_SCAN, first_ctrl->get_type());
  ObSSLobCtrlGCScanTask *first_scan = static_cast<ObSSLobCtrlGCScanTask *>(first_ctrl);
  ASSERT_EQ(0, first_scan->start_tablet_idx_);
  ASSERT_EQ(2, first_scan->end_tablet_idx_);
  first_ctrl->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_SUCCESS, first_ctrl->generate_next_task());
  ObITask *last_ctrl = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(last_ctrl));
  ASSERT_EQ(ObITask::TASK_TYPE_SS_LOB_CTRL_GC_SCAN, last_ctrl->get_type());
  ObSSLobCtrlGCScanTask *last_scan = static_cast<ObSSLobCtrlGCScanTask *>(last_ctrl);
  ASSERT_EQ(2, last_scan->start_tablet_idx_);
  ASSERT_EQ(3, last_scan->end_tablet_idx_);
  last_ctrl->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_SUCCESS, last_ctrl->generate_next_task());
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));
  // 不运行占位扫描；模拟任务完成，验证归并等待所有分区批次。
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(first_ctrl));
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(last_ctrl));
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(ObITask::TASK_TYPE_SS_LOB_CTRL_GC_MERGE_PASS, ready_task->get_type());
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(first_ctrl));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(ObITask::TASK_TYPE_SS_LOB_CTRL_GC_DIFF, ready_task->get_type());
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(first_ctrl));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(ObITask::TASK_TYPE_SS_LOB_CTRL_GC_DELETE, ready_task->get_type());
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(first_ctrl));
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(sort_task, ready_task);
  ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
  ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
  ASSERT_EQ(0, dag.get_task_list_count());
}

TEST_F(TestSSOuttableLobUserGCTask, ctrl_only_batches_accept_finite_and_max_scn)
{
  const int64_t cutoff_values[] = {100, INT64_MAX};
  for (int64_t cutoff : cutoff_values) {
    ObArenaAllocator allocator;
    ObSSMarkSweepDagParam param;
    ASSERT_EQ(OB_SUCCESS, init_param(param));
    param.batch_size_ = 2;
    ObSSMarkSweepDag dag;
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(allocator));
    ASSERT_EQ(OB_SUCCESS, dag.init_by_param(&param));
    ASSERT_EQ(OB_SUCCESS, init_ctrl_scan_inputs(*dag.get_ctx(), 5));
    ASSERT_TRUE(dag.get_ctx()->lob_meta_scan_inputs_.empty());
    if (INT64_MAX == cutoff) {
      dag.get_ctx()->min_outtable_data_scn_.set_max();
    } else {
      ASSERT_EQ(OB_SUCCESS, dag.get_ctx()->min_outtable_data_scn_.convert_for_tx(cutoff));
    }
    ObSSOuttableGCTask *user_task = nullptr;
    ObSSMarkSweepSortTask *sort_task = nullptr;
    ASSERT_EQ(OB_SUCCESS, dag.alloc_task(user_task));
    ASSERT_EQ(OB_SUCCESS, user_task->init());
    ASSERT_EQ(OB_SUCCESS, dag.alloc_task(sort_task));
    ASSERT_EQ(OB_SUCCESS, sort_task->init(0, 1));
    ASSERT_EQ(OB_SUCCESS, user_task->add_child(*sort_task));
    ASSERT_EQ(OB_SUCCESS, dag.add_task(*user_task));
    ASSERT_EQ(OB_SUCCESS, dag.add_task(*sort_task));
    dag.set_dag_status(ObIDag::DAG_STATUS_NODE_RUNNING);
    ObITask *ready_task = nullptr;
    ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
    ASSERT_EQ(user_task, ready_task);
    ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
    ASSERT_EQ(OB_SUCCESS, ready_task->post_generate_next_task());
    ASSERT_EQ(6, dag.get_task_list_count());
    ASSERT_EQ(3, sort_task->get_indegree());
    ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
    ObITask *scan_tasks[3] = {nullptr, nullptr, nullptr};
    for (int64_t i = 0; i < ARRAYSIZEOF(scan_tasks); ++i) {
      ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(scan_tasks[i]));
      ASSERT_EQ(ObITask::TASK_TYPE_SS_LOB_CTRL_GC_SCAN, scan_tasks[i]->get_type());
      ObSSLobCtrlGCScanTask *scan_task = static_cast<ObSSLobCtrlGCScanTask *>(scan_tasks[i]);
      ASSERT_EQ(i * 2, scan_task->start_tablet_idx_);
      ASSERT_EQ(MIN((i + 1) * 2, 5), scan_task->end_tablet_idx_);
      scan_tasks[i]->set_status(ObITask::TASK_STATUS_RUNNING);
      ASSERT_EQ(OB_SUCCESS, scan_tasks[i]->generate_next_task());
    }
    ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));
    // 批次乱序完成时，归并仍等待全部扫描，系统排序仍等待删除后继。
    ASSERT_EQ(OB_SUCCESS, dag.finish_task(scan_tasks[2]));
    ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));
    ASSERT_EQ(OB_SUCCESS, dag.finish_task(scan_tasks[0]));
    ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(ready_task));
    ASSERT_EQ(OB_SUCCESS, dag.finish_task(scan_tasks[1]));
    const ObITask::ObITaskType ctrl_stages[] = {ObITask::TASK_TYPE_SS_LOB_CTRL_GC_MERGE_PASS,
        ObITask::TASK_TYPE_SS_LOB_CTRL_GC_DIFF, ObITask::TASK_TYPE_SS_LOB_CTRL_GC_DELETE};
    for (ObITask::ObITaskType stage : ctrl_stages) {
      ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
      ASSERT_EQ(stage, ready_task->get_type());
      ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
      ObITask *other_task = nullptr;
      ASSERT_EQ(OB_ITER_END, dag.get_next_ready_task(other_task));
      ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
    }
    ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
    ASSERT_EQ(sort_task, ready_task);
    ready_task->set_status(ObITask::TASK_STATUS_RUNNING);
    ASSERT_EQ(OB_SUCCESS, dag.finish_task(ready_task));
    ASSERT_EQ(0, dag.get_task_list_count());
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

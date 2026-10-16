/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#define UNITTEST
#include "storage/compaction/ob_partition_merger.h"
#include "storage/test_tablet_helper.h"
#include "mtlenv/storage/access/test_merge_basic.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace blocksstable;
using namespace compaction;
using namespace unittest;

namespace storage
{

// Verify the structural wiring that lets an async inverted-index sstable flow
// through the merge create path into a tablet's table store:
//   ObStaticMergeParam::is_inv_index_ (signal, set by inverted merge producer)
//   + ObSSTableMergeRes::inv_meta_ (payload)
//     -> ObTabletCreateSSTableParam::init_for_merge (emits INV_INDEX_SSTABLE)
//     -> ObTabletCreateDeleteHelper::create_sstable<ObInvSSTable>
//     -> ObLS::update_tablet_table_store (lands in major_tables_)
class TestInvSSTableMerge : public TestMergeBasic
{
public:
  TestInvSSTableMerge();
  virtual ~TestInvSSTableMerge() {}

  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp() override;
  virtual void TearDown() override;

  void prepare_merge_context(const ObMergeType &merge_type,
                             const bool is_full_merge,
                             const ObVersionRange &trans_version_range,
                             ObTabletMergeCtx &merge_context)
  {
    TestMergeBasic::prepare_merge_context(
        merge_type, is_full_merge, trans_version_range, &merge_dag_, merge_context, false /*is_delete_insert_merge*/);
  }

  void fill_inv_meta(const uint64_t min_doc_id, const uint64_t max_doc_id, ObInvIdxStatsMeta &meta)
  {
    meta.reset();
    meta.min_doc_id_ = min_doc_id;
    meta.max_doc_id_ = max_doc_id;
    meta.doc_id_obj_meta_.set_uint64();
    meta.index_type_ = INV_IDX_TYPE_FTS;
    meta.has_relevance_params_ = true;
    meta.has_position_list_ = true;
    meta.total_doc_length_ = 4096;
  }

  // Build a tiny but real ObSSTableMergeRes by writing one macro block and
  // closing the index builder (mirrors prepare_data_end's res production).
  void build_real_merge_res(const char **micro_data, const int64_t snapshot_version, ObSSTableMergeRes &res)
  {
    reset_writer(snapshot_version);
    prepare_one_macro(micro_data, 1);
    ASSERT_EQ(OB_SUCCESS, macro_writer_.close());
    ASSERT_EQ(OB_SUCCESS, root_index_builder_->close(res));
  }

  ObTabletMergeExecuteDag merge_dag_;
};

void TestInvSSTableMerge::SetUpTestCase()
{
  ObMultiVersionSSTableTest::SetUpTestCase();
  ObClockGenerator::init();
  TestMergeBasic::create_tablet();
}

void TestInvSSTableMerge::TearDownTestCase()
{
  ObMultiVersionSSTableTest::TearDownTestCase();
  ObClockGenerator::destroy();
}

TestInvSSTableMerge::TestInvSSTableMerge()
  : TestMergeBasic("test_inv_sstable_merge")
{}

void TestInvSSTableMerge::SetUp()
{
  ObMultiVersionSSTableTest::SetUp();
}

void TestInvSSTableMerge::TearDown()
{
  ObMultiVersionSSTableTest::TearDown();
}

// Case 1: with the ctx marked as an inverted-index merge, init_for_merge emits
// the inverted-index table type that matches the driving merge type and forwards
// the extended meta from res. Without the flag, it keeps producing a normal minor
// sstable.
TEST_F(TestInvSSTableMerge, init_for_merge_emits_inv_sstable_when_ctx_marks_inv_index)
{
  ObTabletMergeDagParam dag_param;
  ObTabletMergeCtx merge_context(dag_param, allocator_);

  ObTableHandleV2 input_handle;
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -8       0        2        2    T_DML_INSERT  EXIST   CLF\n";
  const int64_t schema_rowkey_cnt = 2;
  const int64_t snapshot_version = 10;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(10);

  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(input_handle);
  merge_context.static_param_.tables_handle_.add_table(input_handle);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = 100;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // The inverted merge producer marks the ctx; this (not res) is the type signal.
  merge_context.static_param_.is_inv_index_ = true;

  // Produce a real merge res, then attach the inverted extended meta as payload.
  ObSSTableMergeRes res;
  build_real_merge_res(micro_data, snapshot_version, res);
  ObInvIdxStatsMeta inv_meta;
  fill_inv_meta(7, 99, inv_meta);
  res.inv_meta_ = inv_meta;

  ObTabletCreateSSTableParam param;
  ASSERT_EQ(OB_SUCCESS, param.init_for_merge(merge_context, res, nullptr /*cg_schema*/, 0 /*cg_idx*/));
  ASSERT_TRUE(param.table_key_.is_inv_sstable());
  ASSERT_TRUE(param.table_key_.is_major_sstable());
  ASSERT_FALSE(param.table_key_.is_inc_major_type_sstable());
  ASSERT_TRUE(param.inv_meta_.is_valid());
  ASSERT_EQ(inv_meta.max_doc_id_, param.inv_meta_.max_doc_id_);
  ASSERT_EQ(inv_meta.min_doc_id_, param.inv_meta_.min_doc_id_);

  // Inc-major inv merge should stay in the inc-major table family instead of
  // being folded into the base major INV_INDEX_SSTABLE type.
  ObTabletMergeDagParam inc_dag_param;
  ObTabletMergeCtx inc_merge_context(inc_dag_param, allocator_);
  inc_merge_context.static_param_.tables_handle_.add_table(input_handle);
  prepare_merge_context(INC_MAJOR_MERGE, false, trans_version_range, inc_merge_context);
  inc_merge_context.static_param_.is_inv_index_ = true;
  ObTabletCreateSSTableParam inc_param;
  ASSERT_EQ(OB_SUCCESS, inc_param.init_for_merge(inc_merge_context, res, nullptr, 0));
  ASSERT_TRUE(inc_param.table_key_.is_inv_sstable());
  ASSERT_TRUE(inc_param.table_key_.is_inc_inv_sstable());
  ASSERT_TRUE(inc_param.table_key_.is_inc_major_type_sstable());
  ASSERT_FALSE(inc_param.table_key_.is_major_sstable());

  // Regression: res.inv_meta_ is no longer the signal. Even when res still carries
  // a valid inv_meta_, clearing the ctx flag must keep producing a normal minor
  // sstable (proves the signal source moved from res to ctx).
  merge_context.static_param_.is_inv_index_ = false;
  ObSSTableMergeRes normal_res;
  build_real_merge_res(micro_data, snapshot_version, normal_res);
  normal_res.inv_meta_ = inv_meta;
  ObTabletCreateSSTableParam normal_param;
  ASSERT_EQ(OB_SUCCESS, normal_param.init_for_merge(merge_context, normal_res, nullptr, 0));
  ASSERT_FALSE(normal_param.table_key_.is_inv_sstable());
}

// Case 2: an inv sstable created via the create_sstable<ObInvSSTable> helper
// can be installed into a real tablet's table store and read back as a
// major-class inv sstable carrying its extended meta.
TEST_F(TestInvSSTableMerge, create_inv_sstable_and_load_into_table_store)
{
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(tablet_id_);
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObTabletHandle tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

  // --- build inv sstable param through the proper merge res path ---
  const char *micro_data[1];
  micro_data[0] =
      "bigint   var   bigint   bigint   bigint bigint dml           flag    multi_version_row_flag\n"
      "0        var1  -8       0        2        2    T_DML_INSERT  EXIST   CLF\n";
  const int64_t schema_rowkey_cnt = 2;
  const int64_t snapshot_version = 100;
  ObScnRange scn_range;
  scn_range.start_scn_.set_min();
  scn_range.end_scn_.convert_for_tx(100);

  prepare_table_schema(micro_data, schema_rowkey_cnt, scn_range, snapshot_version);

  ObTabletMergeDagParam dag_param;
  ObTabletMergeCtx merge_context(dag_param, allocator_);
  ObTableHandleV2 input_handle;
  reset_writer(snapshot_version);
  prepare_one_macro(micro_data, 1);
  prepare_data_end(input_handle);
  merge_context.static_param_.tables_handle_.add_table(input_handle);

  ObVersionRange trans_version_range;
  trans_version_range.snapshot_version_ = snapshot_version;
  trans_version_range.multi_version_start_ = 1;
  trans_version_range.base_version_ = 1;
  prepare_merge_context(MINOR_MERGE, false, trans_version_range, merge_context);
  // The inverted merge producer marks the ctx; this (not res) is the type signal.
  merge_context.static_param_.is_inv_index_ = true;

  ObSSTableMergeRes res;
  build_real_merge_res(micro_data, snapshot_version, res);
  ObInvIdxStatsMeta inv_meta;
  fill_inv_meta(3, 88, inv_meta);
  res.inv_meta_ = inv_meta;

  ObTabletCreateSSTableParam param;
  ASSERT_EQ(OB_SUCCESS, param.init_for_merge(merge_context, res, nullptr /*cg_schema*/, 0 /*cg_idx*/));
  ASSERT_TRUE(param.table_key_.is_inv_sstable());
  ASSERT_TRUE(param.inv_meta_.is_valid());

  ObTableHandleV2 inv_handle;
  ASSERT_EQ(OB_SUCCESS,
            ObTabletCreateDeleteHelper::create_sstable<ObInvSSTable>(param, allocator_, inv_handle));
  ObSSTable *inv_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, inv_handle.get_sstable(inv_sstable));
  ASSERT_NE(nullptr, inv_sstable);
  ASSERT_TRUE(inv_sstable->is_inv_sstable());
  ASSERT_TRUE(inv_sstable->is_major_sstable());
  ASSERT_EQ(88, static_cast<ObInvSSTable *>(inv_sstable)->get_inv_meta().max_doc_id_);
  ASSERT_EQ(3, static_cast<ObInvSSTable *>(inv_sstable)->get_inv_meta().min_doc_id_);

  // --- install into the tablet's table store ---
  ObUpdateTableStoreParam update_param;
  update_param.snapshot_version_ = tablet_handle.get_obj()->get_snapshot_version();
  update_param.multi_version_start_ = tablet_handle.get_obj()->get_multi_version_start();
  ObStorageSchema *schema_on_tablet = nullptr;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->load_storage_schema(allocator_, schema_on_tablet));
  update_param.storage_schema_ = schema_on_tablet;
  update_param.rebuild_seq_ = ls_handle.get_ls()->get_rebuild_seq();
  update_param.ddl_info_.update_with_major_flag_ = false;
  update_param.sstable_ = inv_sstable;
  update_param.allow_duplicate_sstable_ = true;
  ASSERT_EQ(OB_SUCCESS, update_param.init_with_ha_info(ObHATableStoreParam(
      tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_,
      true /*need_check_transfer_seq*/)));
  ASSERT_EQ(OB_SUCCESS, update_param.init_with_compaction_info(ObCompactionTableStoreParam(
      compaction::ObMergeType::MAJOR_MERGE,
      SCN::min_scn() /*clog_checkpoint_scn*/,
      false /*need_report*/,
      tablet_handle.get_obj()->has_merged_with_mds_info())));

  ObTabletHandle new_tablet_handle;
  ASSERT_EQ(OB_SUCCESS,
            ls_handle.get_ls()->update_tablet_table_store(tablet_id, update_param, new_tablet_handle));

  // --- read back from the table store ---
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ASSERT_EQ(OB_SUCCESS, new_tablet_handle.get_obj()->fetch_table_store(table_store_wrapper));
  const ObTabletTableStore *table_store = table_store_wrapper.get_member();
  ASSERT_NE(nullptr, table_store);

  bool found_inv = false;
  for (int64_t i = 0; i < table_store->major_tables_.count(); ++i) {
    ObSSTable *table = table_store->major_tables_.at(i);
    ASSERT_NE(nullptr, table);
    if (table->is_inv_sstable()) {
      found_inv = true;
      ASSERT_EQ(ObStorageMetaValue::MetaType::INV_SSTABLE, table->get_meta_type());
    }
  }
  ASSERT_TRUE(found_inv);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_inv_sstable_merge.log*");
  OB_LOGGER.set_file_name("test_inv_sstable_merge.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

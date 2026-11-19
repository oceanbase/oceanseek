/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_cluster_version.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "storage/lob/store/ob_lob_outtable_io.h"
#include "storage/lob/store/ob_lob_store_common.h"
#include "storage/lob/store/ob_outtable_lob_control_page_iterator.h"
#include "storage/ss_garbage_collector/ob_ss_lob_gc_scan_util.h"
#define private public
#include "storage/ss_data_check/ob_ss_data_check_macro_id_iterator.h"
#include "storage/ss_garbage_collector/ob_ss_mark_sweep_macro_id_iterator.h"
#include "storage/ob_storage_schema.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "lib/utility/ob_defer.h"
#undef private
#undef protected

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;
using namespace blocksstable;

class TestSSMarkSweepMacroIDIterator : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase() { MockTenantModuleEnv::get_instance().destroy(); }

  void SetUp() override
  {
    previous_cluster_version_ = GET_MIN_CLUSTER_VERSION();
    ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_6_3_1);
  }

  void TearDown() override
  {
    ObClusterVersion::get_instance().update_cluster_version(previous_cluster_version_);
  }

  // Borrow fixture-owned schema and store; detach them before the Tablet is destroyed.
  struct TabletFixture
  {
    ObStorageSchema schema_;
    ObTabletTableStore store_;
    ObTablet tablet_;
    void bind()
    {
      store_.is_inited_ = true;
      tablet_.is_inited_ = true;
      tablet_.storage_schema_addr_.ptr_ = &schema_;
      tablet_.table_store_addr_.ptr_ = &store_;
    }
    ~TabletFixture()
    {
      tablet_.storage_schema_addr_.ptr_ = nullptr;
      tablet_.table_store_addr_.ptr_ = nullptr;
      tablet_.is_inited_ = false;
    }
  };

  void prepare_tablet(TabletFixture &f, const ObTableType type, const bool simplified = false)
  {
    ObTableSchema table_schema;
    table_schema.set_tenant_id(OB_SYS_TENANT_ID);
    table_schema.set_database_id(OB_SYS_DATABASE_ID);
    if (AUX_LOB_META == type) {
      ASSERT_EQ(OB_SUCCESS,
          ObInnerTableSchema::all_tenant_profile_aux_lob_meta_schema(table_schema));
    } else {
      ASSERT_EQ(OB_SUCCESS, all_outtable_lob_control_schema(table_schema));
    }
    table_schema.set_table_id(500001);
    table_schema.set_schema_version(100);
    table_schema.set_table_type(type);
    if (USER_INDEX == type) {
      table_schema.set_data_table_id(500000);
      table_schema.set_index_type(INDEX_TYPE_NORMAL_LOCAL);
    }
    ObStorageSchema full_schema;
    ASSERT_EQ(OB_SUCCESS,
        full_schema.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
    ASSERT_EQ(OB_SUCCESS, f.schema_.init(allocator_, full_schema, simplified));
    f.bind();
  }

  void init_iterator(ObSSMarkSweepMacroIDIterator &iter, const int64_t version_count = 1)
  {
    ObSEArray<SCN, 2> versions;
    for (int64_t i = 0; i < version_count; ++i) {
      SCN version;
      ASSERT_EQ(OB_SUCCESS, version.convert_for_tx(i + 1));
      ASSERT_EQ(OB_SUCCESS, versions.push_back(version));
    }
    ASSERT_EQ(OB_SUCCESS, iter.init(sql_proxy_, MTL_ID(), gc_tablet_, versions));
  }

  MacroBlockId lob_id(const int64_t version = 100)
  {
    MacroBlockId id;
    id.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
    id.set_storage_object_type(
        static_cast<uint64_t>(ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO));
    id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
    id.set_second_id(version);
    id.set_third_id(1001);
    id.set_fourth_id(2002);
    return id;
  }

  ObArenaAllocator allocator_;
  ObMySQLProxy sql_proxy_;
  ObSSGCTablet gc_tablet_{ObLSID(1), ObTabletID(200001), SCN::base_scn()};
  uint64_t previous_cluster_version_ = 0;
};

TEST_F(TestSSMarkSweepMacroIDIterator, initialization_and_owned_versions)
{
  ObSSMarkSweepMacroIDIterator iter;
  MacroBlockId id = lob_id();
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(id));
  ASSERT_FALSE(id.is_valid());
  ObSEArray<SCN, 2> versions;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(sql_proxy_, MTL_ID(), gc_tablet_, versions));
  ASSERT_EQ(OB_SUCCESS, versions.push_back(SCN()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(sql_proxy_, MTL_ID(), gc_tablet_, versions));
  versions.at(0) = SCN::base_scn();
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(sql_proxy_, 0, gc_tablet_, versions));
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(sql_proxy_, MTL_ID() + 2, gc_tablet_, versions));
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(sql_proxy_, MTL_ID(), ObSSGCTablet(), versions));
  const ObSSGCTablet no_transfer_scn(ObLSID(1), ObTabletID(200001), SCN());
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(sql_proxy_, MTL_ID(), no_transfer_scn, versions));
  ASSERT_EQ(OB_SUCCESS, iter.init(sql_proxy_, MTL_ID(), gc_tablet_, versions));
  ASSERT_EQ(OB_INIT_TWICE, iter.init(sql_proxy_, MTL_ID(), gc_tablet_, versions));
  versions.reset();
  ASSERT_EQ(1, iter.meta_scns_.count());
  ASSERT_EQ(1, iter.tablet_data_iter_.meta_scn_arr_.count());
  ASSERT_EQ(SCN::base_scn(), iter.meta_scns_.at(0));
}

TEST_F(TestSSMarkSweepMacroIDIterator, data_checker_reports_expansion_error_from_init)
{
  ObSSDataCheckMacroIDIterator iter;
  char buffer[] = "invalid tablet meta";
  const ObString invalid_value = ObString::make_string(buffer);
  MacroBlockId id = lob_id();
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(id));
  ASSERT_FALSE(id.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(ObSSGCTablet(), invalid_value, true));
  const ObSSGCTablet no_transfer_scn(ObLSID(1), ObTabletID(200001), SCN());
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(no_transfer_scn, invalid_value, true));
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init(gc_tablet_, ObString(), true));
  ASSERT_FALSE(iter.is_inited_);
  ObSEArray<ObTabletBlockInfo, 4> blocks;
  const int expected_ret = ObSSMetaTabletMacroIDIterator::collect_blocks_from_tablet_meta_value(
      allocator_, gc_tablet_.ls_id_, gc_tablet_.tablet_id_, gc_tablet_.transfer_scn_,
      invalid_value, false /*fill_occupy_size*/, blocks);
  ASSERT_NE(OB_SUCCESS, expected_ret);
  ASSERT_NE(OB_ITER_END, expected_ret);
  ASSERT_EQ(expected_ret, iter.init(gc_tablet_, invalid_value, true));
  ASSERT_FALSE(iter.is_inited_);
  ASSERT_EQ(invalid_value, iter.meta_value_);
  ASSERT_NE(invalid_value.ptr(), iter.meta_value_.ptr());
  buffer[0] = 'X';
  ASSERT_EQ('i', iter.meta_value_.ptr()[0]);
  ASSERT_FALSE(iter.tablet_handle_.is_valid());
  ASSERT_EQ(nullptr, iter.out_table_data_iter_);
  ASSERT_TRUE(iter.get_known_block_infos().empty());
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(id));
  ASSERT_FALSE(id.is_valid());
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(id));
  ASSERT_FALSE(id.is_valid());
}

TEST_F(TestSSMarkSweepMacroIDIterator, data_checker_keeps_known_blocks_when_lob_preparation_fails)
{
  ObSSDataCheckMacroIDIterator iter;
  iter.gc_tablet_ = gc_tablet_;
  ASSERT_EQ(OB_SUCCESS, ob_write_string(
      iter.allocator_, ObString::make_string("invalid tablet meta"), iter.meta_value_));
  // System expansion and LOB preparation must preserve references collected before a failure.
  const MacroBlockId first(MacroBlockId::DEFAULT_FIRST_ID, 1, 0, 0, 0);
  const MacroBlockId second(MacroBlockId::DEFAULT_FIRST_ID, 2, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, iter.block_infos_.push_back(
      ObTabletBlockInfo(first, ObTabletMacroType::DATA_BLOCK, 0)));
  ASSERT_EQ(OB_SUCCESS, iter.block_infos_.push_back(
      ObTabletBlockInfo(second, ObTabletMacroType::META_BLOCK, 0)));
  const int collect_ret = ObSSMetaTabletMacroIDIterator::collect_blocks_from_tablet_meta_value(
      iter.allocator_, gc_tablet_.ls_id_, gc_tablet_.tablet_id_, gc_tablet_.transfer_scn_,
      iter.meta_value_, false /*fill_occupy_size*/, iter.block_infos_);
  ASSERT_NE(OB_SUCCESS, collect_ret);
  ASSERT_NE(OB_ITER_END, collect_ret);
  ASSERT_EQ(2, iter.get_known_block_infos().count());
  ASSERT_EQ(first, iter.get_known_block_infos().at(0).macro_id());
  ASSERT_EQ(second, iter.get_known_block_infos().at(1).macro_id());
  const int expand_ret = iter.prepare_lob_meta_iterator_();
  ASSERT_NE(OB_SUCCESS, expand_ret);
  ASSERT_NE(OB_ITER_END, expand_ret);
  ASSERT_FALSE(iter.is_inited_);
  ASSERT_FALSE(iter.tablet_handle_.is_valid());
  const ObIArray<ObTabletBlockInfo> &known_blocks = iter.get_known_block_infos();
  ASSERT_EQ(2, known_blocks.count());
  ASSERT_EQ(first, known_blocks.at(0).macro_id());
  ASSERT_EQ(second, known_blocks.at(1).macro_id());
  MacroBlockId id = lob_id();
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(id));
  ASSERT_FALSE(id.is_valid());
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(id));
  ASSERT_EQ(2, iter.get_known_block_infos().count());
}

TEST_F(TestSSMarkSweepMacroIDIterator, data_checker_drains_prepared_blocks_before_meta_eof)
{
  TabletFixture f;
  prepare_tablet(f, AUX_LOB_META);
  ObSSDataCheckMacroIDIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init_lob_meta_iterator_(f.tablet_));
  ASSERT_NE(nullptr, iter.out_table_data_iter_);
  ASSERT_EQ(nullptr, iter.out_table_data_iter_->scan_cache_);
  const MacroBlockId first(MacroBlockId::DEFAULT_FIRST_ID, 1, 0, 0, 0);
  const MacroBlockId second(MacroBlockId::DEFAULT_FIRST_ID, 2, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, iter.block_infos_.push_back(
      ObTabletBlockInfo(first, ObTabletMacroType::DATA_BLOCK, 0)));
  ASSERT_EQ(OB_SUCCESS, iter.block_infos_.push_back(
      ObTabletBlockInfo(second, ObTabletMacroType::META_BLOCK, 0)));
  // Use a fixture-owned Tablet instead of loading shared storage for the successful scan.
  iter.is_inited_ = true;
  ASSERT_EQ(OB_INIT_TWICE,
      iter.init(gc_tablet_, ObString::make_string("invalid tablet meta"), true));
  MacroBlockId id;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(id));
  ASSERT_EQ(first, id);
  ASSERT_EQ(OB_SUCCESS, iter.get_next(id));
  ASSERT_EQ(second, id);
  ASSERT_EQ(OB_ITER_END, iter.get_next(id));
  ASSERT_FALSE(id.is_valid());
  ASSERT_EQ(OB_ITER_END, iter.get_next(id));
}

TEST_F(TestSSMarkSweepMacroIDIterator, data_checker_policy_skips_ctrl_but_keeps_committed_meta)
{
  const MacroBlockId expected = lob_id();
  ObString encoded_id;
  ASSERT_EQ(OB_SUCCESS, ObLobOutTableIO::serialize_macro_id(allocator_, expected, encoded_id));
  ObLobMetaExtraInfo info;
  ObString encoded_info;
  info.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
  info.lob_chunk_type_.set_out_table();
  info.has_crc_ = true;
  info.compressed_len_ = 1;
  ASSERT_EQ(OB_SUCCESS, info.serialize(allocator_, encoded_info));
  for (const ObTableType type : {AUX_LOB_META, AUX_OUTTABLE_LOB_CONTROL}) {
    TabletFixture f;
    prepare_tablet(f, type);
    ObSSDataCheckMacroIDIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init_lob_meta_iterator_(f.tablet_));
    if (AUX_OUTTABLE_LOB_CONTROL == type) {
      // No control status, including DOING, is a required object reference.
      ASSERT_EQ(nullptr, iter.out_table_data_iter_);
    } else {
      ASSERT_NE(nullptr, iter.out_table_data_iter_);
      ASSERT_TRUE(iter.out_table_data_iter_->skip_uncommitted_);
      ObDatumRow physical;
      ASSERT_EQ(OB_SUCCESS, physical.init(allocator_, 9));
      physical.storage_datums_[7].set_string(encoded_info);
      physical.storage_datums_[8].set_string(encoded_id);
      for (const ObDmlFlag flag : {DF_INSERT, DF_UPDATE, DF_DELETE}) {
        physical.row_flag_.set_flag(flag);
        for (const bool uncommitted : {false, true}) {
          physical.mvcc_row_flag_.set_uncommitted_row(uncommitted);
          bool skip = false;
          ASSERT_EQ(OB_SUCCESS, iter.out_table_data_iter_->project_row_(physical, skip));
          ASSERT_EQ(uncommitted, skip);
          if (!skip) {
            MacroBlockId actual;
            ASSERT_EQ(OB_SUCCESS,
                ObLobMetaUtil::get_lob_meta_macro_id(
                    iter.out_table_data_iter_->row_, allocator_, actual));
            ASSERT_EQ(expected, actual);
          }
        }
      }
    }
  }
}

TEST_F(TestSSMarkSweepMacroIDIterator, system_ids_precede_empty_user_scan_and_repeat_eof)
{
  TabletFixture f;
  prepare_tablet(f, AUX_OUTTABLE_LOB_CONTROL);
  ObSSMarkSweepMacroIDIterator iter;
  init_iterator(iter);
  ASSERT_EQ(OB_SUCCESS, iter.init_user_iterator_(f.tablet_));
  ASSERT_NE(nullptr, iter.out_table_data_iter_);
  // Seed an already loaded system version, avoiding SQL and object storage in this unit test.
  iter.tablet_data_iter_.cur_version_idx_ = iter.tablet_data_iter_.meta_scn_arr_.count();
  iter.version_idx_ = iter.meta_scns_.count();
  const MacroBlockId first(MacroBlockId::DEFAULT_FIRST_ID, 1, 0, 0, 0);
  const MacroBlockId second(MacroBlockId::DEFAULT_FIRST_ID, 2, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, iter.tablet_data_iter_.cur_block_infos_.push_back(
      ObTabletBlockInfo(first, ObTabletMacroType::DATA_BLOCK, 0)));
  ASSERT_EQ(OB_SUCCESS, iter.tablet_data_iter_.cur_block_infos_.push_back(
      ObTabletBlockInfo(second, ObTabletMacroType::META_BLOCK, 0)));
  MacroBlockId id;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(id));
  ASSERT_EQ(first, id);
  ASSERT_FALSE(iter.tablet_data_iter_end_);
  ASSERT_NE(nullptr, iter.out_table_data_iter_);
  ASSERT_EQ(OB_SUCCESS, iter.get_next(id));
  ASSERT_EQ(second, id);
  ASSERT_EQ(OB_ITER_END, iter.get_next(id));
  ASSERT_TRUE(iter.tablet_data_iter_end_);
  ASSERT_EQ(nullptr, iter.out_table_data_iter_);
  ASSERT_FALSE(id.is_valid());
  ASSERT_EQ(OB_ITER_END, iter.get_next(id));
}

TEST_F(TestSSMarkSweepMacroIDIterator, meta_projection_distinguishes_nop_null_and_missing_half)
{
  TabletFixture f;
  prepare_tablet(f, AUX_LOB_META);
  ObSSMarkSweepMacroIDIterator iter;
  init_iterator(iter);
  ASSERT_EQ(OB_SUCCESS, iter.init_user_iterator_(f.tablet_));
  ASSERT_NE(nullptr, iter.out_table_data_iter_);
  ObLobColumnIterator &columns = *iter.out_table_data_iter_;
  ASSERT_EQ(2, columns.projector_.count());
  const int64_t info_idx = columns.projector_.at(0);
  const int64_t block_idx = columns.projector_.at(1);
  ObDatumRow source;
  ASSERT_EQ(OB_SUCCESS, source.init(allocator_, columns.column_count_));
  source.row_flag_.set_flag(DF_DELETE);
  source.storage_datums_[info_idx].set_nop();
  source.storage_datums_[block_idx].set_nop();
  bool skip = false;
  ASSERT_EQ(OB_SUCCESS, columns.project_row_(source, skip));
  ASSERT_TRUE(skip);

  source.storage_datums_[info_idx].set_null();
  source.storage_datums_[block_idx].set_null();
  ASSERT_EQ(OB_SUCCESS, columns.project_row_(source, skip));
  ASSERT_FALSE(skip);
  MacroBlockId id = lob_id();
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObLobMetaUtil::get_lob_meta_macro_id(columns.row_, allocator_, id));
  ASSERT_FALSE(id.is_valid());

  source.storage_datums_[info_idx].set_nop();
  ASSERT_EQ(OB_ERR_UNEXPECTED, columns.project_row_(source, skip));
  ASSERT_FALSE(skip);
  source.storage_datums_[info_idx].set_null();
  source.storage_datums_[block_idx].set_nop();
  ASSERT_EQ(OB_ERR_UNEXPECTED, columns.project_row_(source, skip));
  ASSERT_FALSE(skip);
}

TEST_F(TestSSMarkSweepMacroIDIterator, before_version_gate_only_scans_tablet_data)
{
  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_6_1_0);
  ObSSMarkSweepMacroIDIterator iter;
  init_iterator(iter);
  iter.tablet_data_iter_.cur_version_idx_ = iter.tablet_data_iter_.meta_scn_arr_.count();
  const MacroBlockId tablet_block(MacroBlockId::DEFAULT_FIRST_ID, 1, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, iter.tablet_data_iter_.cur_block_infos_.push_back(
      ObTabletBlockInfo(tablet_block, ObTabletMacroType::DATA_BLOCK, 0)));
  MacroBlockId id;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(id));
  ASSERT_EQ(tablet_block, id);
  ASSERT_EQ(OB_ITER_END, iter.get_next(id));
  ASSERT_FALSE(id.is_valid());
  ASSERT_EQ(0, iter.version_idx_);
  ASSERT_EQ(nullptr, iter.out_table_data_iter_);
  ASSERT_EQ(OB_ITER_END, iter.get_next(id));
}

TEST_F(TestSSMarkSweepMacroIDIterator, system_error_is_sticky_and_does_not_start_user_scan)
{
  ObSSMarkSweepMacroIDIterator iter;
  init_iterator(iter);
  iter.tablet_data_iter_.is_inited_ = false;
  MacroBlockId id = lob_id();
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(id));
  ASSERT_FALSE(id.is_valid());
  ASSERT_FALSE(iter.tablet_data_iter_end_);
  ASSERT_EQ(0, iter.version_idx_);
  iter.tablet_data_iter_.is_inited_ = true;
  iter.tablet_data_iter_.iter_end_ = true;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(id));
}

TEST_F(TestSSMarkSweepMacroIDIterator, user_read_error_does_not_become_eof)
{
  TabletFixture f;
  prepare_tablet(f, AUX_OUTTABLE_LOB_CONTROL);
  ObSSTable remote;
  remote.key_.table_type_ = ObITable::REMOTE_LOGICAL_MINOR_SSTABLE;
  remote.key_.tablet_id_ = ObTabletID(200001);
  ObSSMarkSweepMacroIDIterator iter;
  init_iterator(iter);
  ASSERT_EQ(OB_SUCCESS, iter.init_user_iterator_(f.tablet_));
  iter.out_table_data_iter_->tables_.need_load_sstable_ = false;
  ASSERT_EQ(OB_SUCCESS, iter.out_table_data_iter_->tables_.add_table(&remote));
  iter.tablet_data_iter_.iter_end_ = true;
  iter.version_idx_ = iter.meta_scns_.count();
  MacroBlockId id = lob_id();
  ASSERT_EQ(OB_NOT_SUPPORTED, iter.get_next(id));
  ASSERT_FALSE(id.is_valid());
  ASSERT_TRUE(iter.tablet_data_iter_end_);
  ASSERT_FALSE(iter.lob_scan_cache_.contains(remote.get_key()));
  ASSERT_EQ(OB_NOT_SUPPORTED, iter.get_next(id));
}

TEST_F(TestSSMarkSweepMacroIDIterator, sstable_scan_cache_survives_version_switch_and_isolates_groups)
{
  for (const ObTableType type : {AUX_LOB_META, AUX_OUTTABLE_LOB_CONTROL}) {
    TabletFixture f;
    prepare_tablet(f, type);
    ObTabletCreateSSTableParam param;
    ASSERT_EQ(OB_SUCCESS, param.init_for_empty_major_sstable(gc_tablet_.tablet_id_, f.schema_,
        100 /*snapshot*/, -1 /*cg_idx*/, false, false));
    void *buffer = allocator_.alloc(sizeof(ObSSTable));
    ASSERT_NE(nullptr, buffer);
    ObSSTable *table = new (buffer) ObSSTable();
    bool owned = false;
    DEFER(if (!owned) { table->~ObSSTable(); });
    ASSERT_EQ(OB_SUCCESS, table->init(param, &allocator_));
    ObTableHandleV2 handle;
    ASSERT_EQ(OB_SUCCESS, handle.set_sstable(table, &allocator_));
    owned = true;
    ObSEArray<ObITable *, 1> tables;
    ASSERT_EQ(OB_SUCCESS, tables.push_back(table));
    ASSERT_EQ(OB_SUCCESS, f.store_.major_tables_.init(allocator_, tables));

    ObSSMarkSweepMacroIDIterator del_iter;
    init_iterator(del_iter, 2);
    // Supply each loaded Tablet directly, avoiding shared-meta I/O in this unit test.
    del_iter.version_idx_ = del_iter.meta_scns_.count();
    ASSERT_EQ(OB_SUCCESS, del_iter.init_user_iterator_(f.tablet_));
    ASSERT_EQ(&del_iter.lob_scan_cache_, del_iter.out_table_data_iter_->scan_cache_);
    ASSERT_FALSE(del_iter.lob_scan_cache_.scanned_sstables_.created());
    MacroBlockId id;
    ASSERT_EQ(OB_ITER_END, del_iter.get_next_user_macro_id_(id));
    ASSERT_EQ(nullptr, del_iter.out_table_data_iter_);
    ASSERT_TRUE(del_iter.lob_scan_cache_.contains(table->get_key()));

    // The next version references the same file. Corrupt its layout to detect any re-open.
    ObSSTable *stored_table = f.store_.major_tables_[0];
    --stored_table->meta_->basic_meta_.column_cnt_;
    DEFER(++stored_table->meta_->basic_meta_.column_cnt_);
    ASSERT_EQ(OB_SUCCESS, del_iter.init_user_iterator_(f.tablet_));
    ASSERT_EQ(OB_ITER_END, del_iter.get_next_user_macro_id_(id));
    ASSERT_FALSE(id.is_valid());
    ASSERT_EQ(nullptr, del_iter.out_table_data_iter_);

    // KEPT must read the file independently, even if DEL has already scanned it.
    ObSSMarkSweepMacroIDIterator kept_iter;
    init_iterator(kept_iter, 2);
    kept_iter.version_idx_ = kept_iter.meta_scns_.count();
    ASSERT_EQ(OB_SUCCESS, kept_iter.init_user_iterator_(f.tablet_));
    ASSERT_EQ(OB_NOT_SUPPORTED, kept_iter.get_next_user_macro_id_(id));
    ASSERT_FALSE(id.is_valid());
    ASSERT_FALSE(kept_iter.lob_scan_cache_.contains(table->get_key()));
  }
}

TEST_F(TestSSMarkSweepMacroIDIterator, actual_meta_and_ctrl_schema_projection)
{
  for (const ObTableType type : {AUX_LOB_META, AUX_OUTTABLE_LOB_CONTROL}) {
    for (const bool simplified : {false, true}) {
      TabletFixture f;
      prepare_tablet(f, type, simplified);
      ASSERT_EQ(OB_SUCCESS, f.tablet_.table_store_cache_.init(f.store_, f.schema_, 0));
      ObSSMarkSweepMacroIDIterator iter;
      ASSERT_EQ(OB_SUCCESS, iter.init_user_iterator_(f.tablet_));
      ASSERT_NE(nullptr, iter.out_table_data_iter_);
      ASSERT_EQ(AUX_LOB_META == type, iter.is_lob_meta_);
      ASSERT_EQ((AUX_LOB_META == type) ? 2 : 1, iter.out_table_data_iter_->columns_.count());
      ASSERT_EQ((AUX_LOB_META == type) ? 7 : 0, iter.out_table_data_iter_->projector_.at(0));
      if (AUX_LOB_META == type) {
        ASSERT_EQ(8, iter.out_table_data_iter_->projector_.at(1));
        ObSSDataCheckMacroIDIterator check_iter;
        ASSERT_EQ(OB_SUCCESS, check_iter.init_lob_meta_iterator_(f.tablet_));
        ASSERT_NE(nullptr, check_iter.out_table_data_iter_);
        ASSERT_TRUE(check_iter.out_table_data_iter_->skip_uncommitted_);
        ASSERT_EQ(2, check_iter.out_table_data_iter_->projector_.count());
        ASSERT_EQ(7, check_iter.out_table_data_iter_->projector_.at(0));
        ASSERT_EQ(8, check_iter.out_table_data_iter_->projector_.at(1));
      }
      MacroBlockId id;
      ASSERT_EQ(OB_ITER_END, iter.get_next_user_macro_id_(id));
      ASSERT_EQ(nullptr, iter.out_table_data_iter_);
    }
  }
}

TEST_F(TestSSMarkSweepMacroIDIterator, column_iterator_still_validates_meta_requests)
{
  for (const bool simplified : {false, true}) {
    TabletFixture f;
    prepare_tablet(f, AUX_LOB_META, simplified);
    ObSEArray<ObColDesc, ObLobMetaUtil::LOB_META_MACRO_ID_COLUMN_CNT> columns;
    ASSERT_EQ(OB_SUCCESS, ObSSLobGCScanUtil::prepare_meta_scan_columns(columns));
    columns.at(0).col_id_ = OB_APP_MIN_COLUMN_ID + f.schema_.get_column_count();
    ObLobColumnIterator missing_column_iter;
    ASSERT_EQ(OB_INVALID_ARGUMENT, missing_column_iter.init(f.tablet_, columns, false));
    if (!simplified) {
      ASSERT_EQ(OB_SUCCESS, ObSSLobGCScanUtil::prepare_meta_scan_columns(columns));
      columns.at(0).col_type_.set_int();
      ObLobColumnIterator wrong_type_iter;
      ASSERT_EQ(OB_INVALID_ARGUMENT, wrong_type_iter.init(f.tablet_, columns, false));
    }
  }
  TabletFixture f;
  prepare_tablet(f, USER_TABLE);
  ObSEArray<ObColDesc, ObLobMetaUtil::LOB_META_MACRO_ID_COLUMN_CNT> columns;
  ASSERT_EQ(OB_SUCCESS, ObSSLobGCScanUtil::prepare_meta_scan_columns(columns));
  ObLobColumnIterator wrong_table_iter;
  ASSERT_EQ(OB_NOT_SUPPORTED, wrong_table_iter.init(f.tablet_, columns, false));
}

TEST_F(TestSSMarkSweepMacroIDIterator, gc_retains_uncommitted_references_across_tablet_switch)
{
  ObSEArray<SCN, 2> versions;
  ASSERT_EQ(OB_SUCCESS, versions.push_back(SCN::base_scn()));
  const MacroBlockId expected = lob_id();
  ObString encoded_id;
  ASSERT_EQ(OB_SUCCESS, ObLobOutTableIO::serialize_macro_id(allocator_, expected, encoded_id));
  ObLobMetaExtraInfo info;
  ObString encoded_info;
  info.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
  info.lob_chunk_type_.set_out_table();
  info.has_crc_ = true;
  info.compressed_len_ = 1;
  ASSERT_EQ(OB_SUCCESS, info.serialize(allocator_, encoded_info));
  for (const ObTableType type : {AUX_LOB_META, AUX_OUTTABLE_LOB_CONTROL}) {
    TabletFixture f;
    prepare_tablet(f, type);
    const bool is_lob_meta = AUX_LOB_META == type;
    ObDatumRow physical;
    ASSERT_EQ(OB_SUCCESS, physical.init(allocator_, is_lob_meta ? 9 : 7));
    physical.row_flag_.set_flag(DF_INSERT);
    if (is_lob_meta) {
      physical.storage_datums_[7].set_string(encoded_info);
      physical.storage_datums_[8].set_string(encoded_id);
    } else {
      physical.storage_datums_[0].set_string(encoded_id);
    }
    ObSSMarkSweepMacroIDIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init(sql_proxy_, MTL_ID(), gc_tablet_, versions));
    for (int64_t tablet_idx = 0; tablet_idx < 2; ++tablet_idx) {
      ASSERT_EQ(OB_SUCCESS, iter.init_user_iterator_(f.tablet_));
      ASSERT_NE(nullptr, iter.out_table_data_iter_);
      ASSERT_FALSE(iter.out_table_data_iter_->skip_uncommitted_);
      for (const bool uncommitted : {false, true}) {
        physical.mvcc_row_flag_.set_uncommitted_row(uncommitted);
        bool skip = false;
        ASSERT_EQ(OB_SUCCESS, iter.out_table_data_iter_->project_row_(physical, skip));
        ASSERT_FALSE(skip);
        MacroBlockId actual;
        const ObDatumRow &row = iter.out_table_data_iter_->row_;
        const int ret = is_lob_meta
            ? ObLobMetaUtil::get_lob_meta_macro_id(row, allocator_, actual)
            : ObOuttableLobControlUtil::get_lob_ctrl_macro_id(row, actual);
        ASSERT_EQ(OB_SUCCESS, ret);
        ASSERT_EQ(expected, actual);
      }
      iter.reset_user_iterator_();
    }
  }
}

TEST_F(TestSSMarkSweepMacroIDIterator, skips_normal_index_and_empty_shell_tablets)
{
  for (const ObTableType type : {USER_TABLE, USER_INDEX}) {
    TabletFixture f;
    prepare_tablet(f, type);
    ObSSMarkSweepMacroIDIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.init_user_iterator_(f.tablet_));
    ASSERT_EQ(nullptr, iter.out_table_data_iter_);
  }
  ObTablet empty;
  empty.tablet_meta_.is_empty_shell_ = true;
  ObSSMarkSweepMacroIDIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init_user_iterator_(empty));
  ASSERT_EQ(nullptr, iter.out_table_data_iter_);
}

TEST_F(TestSSMarkSweepMacroIDIterator, cached_types_skip_unneeded_schema_loads)
{
  for (const ObTableType type : {USER_TABLE, USER_INDEX, AUX_LOB_PIECE}) {
    ObTablet tablet;
    tablet.table_store_cache_.table_type_ = type;
    // These tablets have no schema or table store; classification must use the cache only.
    ObSSMarkSweepMacroIDIterator gc_iter;
    ASSERT_EQ(OB_SUCCESS, gc_iter.init_user_iterator_(tablet));
    ASSERT_EQ(nullptr, gc_iter.out_table_data_iter_);
    ObSSDataCheckMacroIDIterator check_iter;
    ASSERT_EQ(OB_SUCCESS, check_iter.init_lob_meta_iterator_(tablet));
    ASSERT_EQ(nullptr, check_iter.out_table_data_iter_);
  }
  ObTablet ctrl_tablet;
  ctrl_tablet.table_store_cache_.table_type_ = AUX_OUTTABLE_LOB_CONTROL;
  ObSSDataCheckMacroIDIterator check_iter;
  ASSERT_EQ(OB_SUCCESS, check_iter.init_lob_meta_iterator_(ctrl_tablet));
  ASSERT_EQ(nullptr, check_iter.out_table_data_iter_);
}

TEST_F(TestSSMarkSweepMacroIDIterator, unknown_type_uses_schema_without_updating_cache)
{
  for (const ObTableType type : {USER_TABLE, USER_INDEX, AUX_LOB_META, AUX_OUTTABLE_LOB_CONTROL}) {
    TabletFixture f;
    prepare_tablet(f, type);
    ASSERT_EQ(MAX_TABLE_TYPE, f.tablet_.table_store_cache_.get_table_type());
    ObSSMarkSweepMacroIDIterator gc_iter;
    ASSERT_EQ(OB_SUCCESS, gc_iter.init_user_iterator_(f.tablet_));
    ASSERT_EQ(AUX_LOB_META == type || AUX_OUTTABLE_LOB_CONTROL == type,
        nullptr != gc_iter.out_table_data_iter_);
    ObSSDataCheckMacroIDIterator check_iter;
    ASSERT_EQ(OB_SUCCESS, check_iter.init_lob_meta_iterator_(f.tablet_));
    ASSERT_EQ(AUX_LOB_META == type, nullptr != check_iter.out_table_data_iter_);
    ASSERT_EQ(MAX_TABLE_TYPE, f.tablet_.table_store_cache_.get_table_type());
  }
}

TEST_F(TestSSMarkSweepMacroIDIterator, unknown_type_without_schema_is_an_error)
{
  ObTablet tablet;
  ASSERT_EQ(MAX_TABLE_TYPE, tablet.table_store_cache_.get_table_type());
  ObSSMarkSweepMacroIDIterator gc_iter;
  ASSERT_EQ(OB_INVALID_ARGUMENT, gc_iter.init_user_iterator_(tablet));
  ObSSDataCheckMacroIDIterator check_iter;
  ASSERT_EQ(OB_INVALID_ARGUMENT, check_iter.init_lob_meta_iterator_(tablet));

  tablet.tablet_meta_.is_empty_shell_ = true;
  ASSERT_EQ(OB_SUCCESS, gc_iter.init_user_iterator_(tablet));
  ASSERT_EQ(OB_SUCCESS, check_iter.init_lob_meta_iterator_(tablet));
  ASSERT_EQ(nullptr, gc_iter.out_table_data_iter_);
  ASSERT_EQ(nullptr, check_iter.out_table_data_iter_);
}

TEST_F(TestSSMarkSweepMacroIDIterator, schema_and_table_store_errors_are_not_empty_scans)
{
  TabletFixture f;
  prepare_tablet(f, AUX_LOB_META);
  ASSERT_EQ(OB_SUCCESS, f.tablet_.table_store_cache_.init(f.store_, f.schema_, 0));
  f.store_.is_inited_ = false;
  {
    ObSSMarkSweepMacroIDIterator iter;
    ASSERT_EQ(OB_NOT_INIT, iter.init_user_iterator_(f.tablet_));
  }
  f.store_.is_inited_ = true;
  f.tablet_.storage_schema_addr_.ptr_ = nullptr;
  ObSSMarkSweepMacroIDIterator iter;
  ASSERT_EQ(OB_INVALID_ARGUMENT, iter.init_user_iterator_(f.tablet_));
  ObSSDataCheckMacroIDIterator check_iter;
  ASSERT_EQ(OB_INVALID_ARGUMENT, check_iter.init_lob_meta_iterator_(f.tablet_));
}

TEST_F(TestSSMarkSweepMacroIDIterator, ctrl_preserves_physical_flags_versions_and_statuses)
{
  TabletFixture f;
  prepare_tablet(f, AUX_OUTTABLE_LOB_CONTROL);
  ObSSMarkSweepMacroIDIterator iter;
  ASSERT_EQ(OB_SUCCESS, iter.init_user_iterator_(f.tablet_));
  ObDatumRow physical;
  ASSERT_EQ(OB_SUCCESS, physical.init(allocator_, 7));
  const ObDmlFlag flags[] = {DF_INSERT, DF_DELETE, DF_UPDATE, DF_DELETE};
  for (int64_t i = 0; i < ARRAYSIZEOF(flags); ++i) {
    physical.row_flag_.set_flag(flags[i]);
    physical.mvcc_row_flag_.set_uncommitted_row(i == 2);
    physical.mvcc_row_flag_.set_shadow_row(i == 1);
    physical.mvcc_row_flag_.set_ghost_row(i == 3);
    const MacroBlockId expected = lob_id(100 + i);
    ObString encoded;
    ASSERT_EQ(OB_SUCCESS, ObLobOutTableIO::serialize_macro_id(allocator_, expected, encoded));
    physical.storage_datums_[0].set_string(encoded);
    physical.storage_datums_[2].set_int(-100 - i);
    physical.storage_datums_[4].set_int(i % 3); // All control statuses are retained.
    bool skip = true;
    ASSERT_EQ(OB_SUCCESS, iter.out_table_data_iter_->project_row_(physical, skip));
    ASSERT_FALSE(skip);
    MacroBlockId actual;
    ASSERT_EQ(OB_SUCCESS, ObOuttableLobControlUtil::get_lob_ctrl_macro_id(
        iter.out_table_data_iter_->row_, actual));
    ASSERT_EQ(expected, actual);
  }
}

TEST_F(TestSSMarkSweepMacroIDIterator, ctrl_rejects_null_malformed_and_trailing_bytes)
{
  ObDatumRow row;
  MacroBlockId id;
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObOuttableLobControlUtil::get_lob_ctrl_macro_id(row, id));
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 1));
  row.storage_datums_[0].set_null();
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObOuttableLobControlUtil::get_lob_ctrl_macro_id(row, id));
  row.storage_datums_[0].set_string("bad", 3);
  ASSERT_NE(OB_SUCCESS, ObOuttableLobControlUtil::get_lob_ctrl_macro_id(row, id));
  row.storage_datums_[0].set_string("", 0);
  ASSERT_NE(OB_SUCCESS, ObOuttableLobControlUtil::get_lob_ctrl_macro_id(row, id));
  char buffer[128] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, lob_id().serialize(buffer, sizeof(buffer), pos));
  row.storage_datums_[0].set_string(buffer, pos + 1);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObOuttableLobControlUtil::get_lob_ctrl_macro_id(row, id));
  pos = 0;
  MacroBlockId wrong_type(MacroBlockId::DEFAULT_FIRST_ID, 1, 0, 0, 0);
  ASSERT_EQ(OB_SUCCESS, wrong_type.serialize(buffer, sizeof(buffer), pos));
  row.storage_datums_[0].set_string(buffer, pos);
  ASSERT_NE(OB_SUCCESS, ObOuttableLobControlUtil::get_lob_ctrl_macro_id(row, id));
}

TEST_F(TestSSMarkSweepMacroIDIterator, meta_outtable_and_non_outtable_rows)
{
  ObDatumRow row;
  MacroBlockId id = lob_id();
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 2));
  row.storage_datums_[0].set_null();
  row.storage_datums_[1].set_null();
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator_, id));
  ASSERT_FALSE(id.is_valid());
  ObLobMetaExtraInfo info;
  ObString encoded_info;
  info.extra_info_type_ = ObLobMetaExtraInfo::LOB_DATA;
  info.lob_chunk_type_.set_in_table();
  ASSERT_EQ(OB_SUCCESS, info.serialize(allocator_, encoded_info));
  row.storage_datums_[0].set_string(encoded_info);
  ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator_, id));
  ASSERT_FALSE(id.is_valid());
  info.lob_chunk_type_.set_out_table();
  info.has_crc_ = true;
  info.compressed_len_ = 1;
  ASSERT_EQ(OB_SUCCESS, info.serialize(allocator_, encoded_info));
  row.storage_datums_[0].set_string(encoded_info);
  ObString encoded_id;
  ASSERT_EQ(OB_SUCCESS, ObLobOutTableIO::serialize_macro_id(allocator_, lob_id(), encoded_id));
  row.storage_datums_[1].set_string(encoded_id);
  for (const ObDmlFlag flag : {DF_INSERT, DF_UPDATE, DF_DELETE}) {
    row.row_flag_.set_flag(flag);
    row.mvcc_row_flag_.set_uncommitted_row(true);
    ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator_, id));
    ASSERT_EQ(lob_id(), id);
  }
  row.storage_datums_[1].set_null();
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator_, id));
  row.storage_datums_[1].set_string("bad", 3);
  ASSERT_NE(OB_SUCCESS, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator_, id));
  row.storage_datums_[0].set_null();
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator_, id));
  row.storage_datums_[0].set_string("invalid json", 12);
  ASSERT_NE(OB_SUCCESS, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator_, id));
  row.count_ = 1;
  ASSERT_EQ(OB_ERR_UNEXPECTED, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator_, id));
}

TEST_F(TestSSMarkSweepMacroIDIterator, meta_summary_and_external_blob_are_not_outtable_objects)
{
  ObDatumRow row;
  MacroBlockId id = lob_id();
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, 2));
  ObLobMetaExtraInfo info;
  info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  info.lob_chunk_type_.set_out_table();
  info.chunk_count_ = 1;
  info.compressed_len_ = 1;
  ObString encoded_info;
  ASSERT_EQ(OB_SUCCESS, info.serialize(allocator_, encoded_info));
  row.storage_datums_[0].set_string(encoded_info);
  row.storage_datums_[1].set_null();
  ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator_, id));
  ASSERT_FALSE(id.is_valid());
  ObExternalBlobInfo external;
  external.url_ = ObString::make_string("s3://test/external_lob");
  external.length_ = 16;
  external.kind_ = ObExternalBlobKind::REFERENCE;
  info.reset();
  info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  ASSERT_EQ(OB_SUCCESS, info.set_external_lob_info(allocator_, external));
  ASSERT_EQ(OB_SUCCESS, info.serialize(allocator_, encoded_info));
  ObString external_id;
  ASSERT_EQ(OB_SUCCESS,
      ObExternalLobMacroIdPrefix::serialize(allocator_, 200001, 10, external_id));
  row.storage_datums_[0].set_string(encoded_info);
  row.storage_datums_[1].set_string(external_id);
  ASSERT_EQ(OB_SUCCESS, ObLobMetaUtil::get_lob_meta_macro_id(row, allocator_, id));
  ASSERT_FALSE(id.is_valid());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

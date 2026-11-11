/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "storage/blocksstable/ob_storage_datum.h"
#include "storage/direct_load/ob_direct_load_insert_table_ctx.h"
#include "storage/direct_load/ob_direct_load_lob_meta_row_iter.h"
#include "storage/direct_load/ob_direct_load_merge_ctx.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"
#include "storage/direct_load/ob_direct_load_tmp_file.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "unittest/storage/init_basic_struct.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace storage;

class OriginTableAccessorForTest : public ObDirectLoadOriginTableAccessor
{
public:
  ~OriginTableAccessorForTest() override { reset(); }
  int init_access_ctx(ObDirectLoadOriginTable *origin_table)
  {
    origin_table_ = origin_table;
    return init_table_access_ctx(true /*skip_read_lob*/);
  }
  int64_t get_access_snapshot_version() const
  {
    return table_access_ctx_.trans_version_range_.snapshot_version_;
  }
  int64_t get_store_snapshot_version() const
  {
    return store_ctx_.mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx();
  }
protected:
  int inner_get_next_row(const ObDirectLoadDatumRow *&datum_row) override
  {
    UNUSED(datum_row);
    return OB_NOT_SUPPORTED;
  }
  int reinit_open() override { return OB_NOT_SUPPORTED; }
};

class TestDirectLoadOriginTable : public ::testing::Test
{
public:
  static void SetUpTestCase();
  static void TearDownTestCase();
  static void fill_valid_param(const int64_t snapshot_version,
                               const ObIArray<share::schema::ObColDesc> &col_descs,
                               ObDirectLoadOriginTableCreateParam &param)
  {
    param.table_id_ = 500001;
    param.snapshot_version_ = snapshot_version;
    param.tablet_id_ = ObTabletID(200001);
    param.ls_id_ = ObLSID(2000);
    param.rowkey_column_num_ = 1;
    param.col_descs_ = &col_descs;
  }
};

void TestDirectLoadOriginTable::SetUpTestCase()
{
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
  ObCreateLSArg create_ls_arg;
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(1, ObLSID(2000), create_ls_arg));
  ASSERT_EQ(OB_SUCCESS, MTL(ObLSService *)->create_ls(create_ls_arg));
}

void TestDirectLoadOriginTable::TearDownTestCase()
{
  EXPECT_EQ(OB_SUCCESS, MTL(ObLSService *)->remove_ls(ObLSID(2000)));
  MockTenantModuleEnv::get_instance().destroy();
}

// 验证 origin table meta 只接受并保存上层冻结的 DDL snapshot。
TEST_F(TestDirectLoadOriginTable, preserve_frozen_snapshot)
{
  const int64_t snapshot_version = 1787397290899322003;
  ObArray<share::schema::ObColDesc> col_descs;
  ObDirectLoadOriginTableCreateParam param;
  fill_valid_param(snapshot_version, col_descs, param);

  ObDirectLoadOriginTable origin_table;
  ASSERT_EQ(OB_SUCCESS, origin_table.init(param));
  EXPECT_EQ(snapshot_version, origin_table.get_meta().snapshot_version_);

  origin_table.reset();
  EXPECT_EQ(0, origin_table.get_meta().snapshot_version_);

  param.snapshot_version_ = 0;
  EXPECT_FALSE(param.is_valid());
  ObDirectLoadOriginTable invalid_origin_table;
  EXPECT_EQ(OB_INVALID_ARGUMENT, invalid_origin_table.init(param));
}

TEST_F(TestDirectLoadOriginTable, merge_param_requires_snapshot)
{
  ObArray<share::schema::ObColDesc> col_descs;
  blocksstable::ObStorageDatumUtils datum_utils;
  ObArray<int64_t> lob_column_idxs;
  ObDirectLoadInsertTableContext insert_table_ctx;
  ObDirectLoadTmpFileManager file_mgr;
  observer::ObTableLoadTableCtx table_ctx;
  ObDirectLoadMergeParam param;
  param.table_id_ = 500001;
  param.snapshot_version_ = 1787397290899322003;
  param.rowkey_column_num_ = 1;
  param.column_count_ = 1;
  param.col_descs_ = &col_descs;
  param.datum_utils_ = &datum_utils;
  param.lob_column_idxs_ = &lob_column_idxs;
  param.merge_mode_ = ObDirectLoadMergeMode::NORMAL;
  param.insert_table_ctx_ = &insert_table_ctx;
  param.file_mgr_ = &file_mgr;
  param.ctx_ = &table_ctx;
  ASSERT_TRUE(param.is_valid());

  param.snapshot_version_ = 0;
  EXPECT_FALSE(param.is_valid());
}

TEST_F(TestDirectLoadOriginTable, access_ctx_uses_frozen_snapshot)
{
  const int64_t snapshot_version = 1787397290899322003;
  ObArray<share::schema::ObColDesc> col_descs;
  ObDirectLoadOriginTableCreateParam param;
  fill_valid_param(snapshot_version, col_descs, param);
  ObDirectLoadOriginTable origin_table;
  ASSERT_EQ(OB_SUCCESS, origin_table.init(param));

  OriginTableAccessorForTest accessor;
  ASSERT_EQ(OB_SUCCESS, accessor.init_access_ctx(&origin_table));
  EXPECT_EQ(snapshot_version, accessor.get_access_snapshot_version());
  EXPECT_EQ(snapshot_version, accessor.get_store_snapshot_version());
}

class TestDirectLoadLobAuxRowIter : public ::testing::Test
{
protected:
  void SetUp() override;

  blocksstable::ObStorageDatumUtils datum_utils_;
  ObDirectLoadLobAuxKeyConflictHandler conflict_handler_;
  ObDirectLoadMultipleDatumRange range_;
  ObDirectLoadDatumRow completed_key_;
  ObDirectLoadLobAuxRowIter iter_;
};

void TestDirectLoadLobAuxRowIter::SetUp()
{
  ObDirectLoadMultipleSSTableScanMergeParam param;
  param.table_data_desc_.rowkey_column_num_ = 2;
  param.table_data_desc_.column_count_ = 2;
  param.table_data_desc_.external_data_block_size_ = DIO_ALIGN_SIZE;
  param.table_data_desc_.sstable_index_block_size_ = DIO_ALIGN_SIZE;
  param.table_data_desc_.sstable_data_block_size_ = DIO_ALIGN_SIZE;
  param.table_data_desc_.extra_buf_size_ = DIO_ALIGN_SIZE;
  param.table_data_desc_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.datum_utils_ = &datum_utils_;
  param.dml_row_handler_ = &conflict_handler_;
  range_.set_whole_range();
  ObDirectLoadTableHandleArray tables;
  ASSERT_EQ(OB_SUCCESS, iter_.scan_merge_.init(param, tables, range_));
  ASSERT_EQ(OB_SUCCESS, completed_key_.init(2));
  completed_key_.storage_datums_[0].set_int(0);
  completed_key_.storage_datums_[1].set_string(ObString::make_string("completed_lob"));
  completed_key_.is_delete_ = true;
  void *scanner_buf = iter_.allocator_.alloc(sizeof(ObDirectLoadOriginTableScanner));
  ASSERT_NE(nullptr, scanner_buf);
  iter_.origin_scanner_ = new (scanner_buf) ObDirectLoadOriginTableScanner();
  iter_.origin_scanner_->schema_param_.table_type_ = share::schema::AUX_LOB_META;
  // Resume at the boundary after the last auxiliary key's physical scan has ended.
  iter_.lob_aux_key_row_ = &completed_key_;
}

TEST_F(TestDirectLoadLobAuxRowIter, outtable_piece_delete_reaches_end_without_piece_rows)
{
  iter_.origin_scanner_->schema_param_.table_type_ = share::schema::AUX_LOB_PIECE;
  EXPECT_EQ(OB_ITER_END, iter_.switch_next_lob_aux_key());
}

TEST_F(TestDirectLoadLobAuxRowIter, missing_meta_delete_remains_an_error)
{
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter_.switch_next_lob_aux_key());
}

TEST_F(TestDirectLoadLobAuxRowIter, missing_piece_insert_remains_an_error)
{
  iter_.origin_scanner_->schema_param_.table_type_ = share::schema::AUX_LOB_PIECE;
  completed_key_.is_delete_ = false;
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter_.switch_next_lob_aux_key());
}

TEST_F(TestDirectLoadLobAuxRowIter, existing_auxiliary_rows_still_finish_normally)
{
  iter_.lob_aux_key_row_cnt_ = 1;
  EXPECT_EQ(OB_ITER_END, iter_.switch_next_lob_aux_key());
}

TEST_F(TestDirectLoadLobAuxRowIter, missing_scanner_only_allows_initial_switch)
{
  ObDirectLoadOriginTableScanner *scanner = iter_.origin_scanner_;
  iter_.origin_scanner_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter_.switch_next_lob_aux_key());
  iter_.lob_aux_key_row_ = nullptr;
  EXPECT_EQ(OB_ITER_END, iter_.switch_next_lob_aux_key());
  iter_.origin_scanner_ = scanner;
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

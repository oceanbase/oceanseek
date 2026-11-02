/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#define UNITTEST_DEBUG
#define USING_LOG_PREFIX STORAGE
#include <gmock/gmock.h>

#define private public
#define protected public

#include "storage/mockcontainer/mock_ob_iterator.h"
#include "storage/ob_storage_struct.h"
#include "storage/ob_storage_schema_util.h"
#include "storage/schema_utils.h"
#include "storage/async_index/ob_async_index_define.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tablet/ob_table_store_util.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "share/ob_server_struct.h"
#include "share/ob_tablet_replica_checksum_operator.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/utility/ob_defer.h"


namespace oceanbase
{
using namespace common;
using namespace storage;
using namespace blocksstable;
using namespace memtable;
using namespace share::schema;
using namespace share;
using namespace compaction;
using namespace transaction;

namespace storage
{
  bool ObUpdateTableStoreParam::is_valid() const { return true; }
}

namespace unittest
{

namespace
{
class ServerModeGuard final
{
public:
  explicit ServerModeGuard(const share::ObServerMode mode)
    : old_mode_(GCTX.startup_mode_)
  {
    GCTX.startup_mode_ = mode;
  }
  ~ServerModeGuard()
  {
    GCTX.startup_mode_ = old_mode_;
  }
private:
  share::ObServerMode old_mode_;
};
}

ObITable::TableType TYPE_MAP[] = {
  ObITable::TableType::DATA_MEMTABLE,
  ObITable::TableType::TX_DATA_MEMTABLE,
  ObITable::TableType::TX_CTX_MEMTABLE,
  ObITable::TableType::LOCK_MEMTABLE,
  ObITable::TableType::DIRECT_LOAD_MEMTABLE,
  ObITable::TableType::MAX_MEMTABLE_TYPE,
  ObITable::TableType::MAX_MEMTABLE_TYPE,
  ObITable::TableType::MAX_MEMTABLE_TYPE,
  ObITable::TableType::MAX_MEMTABLE_TYPE,
  ObITable::TableType::MAX_MEMTABLE_TYPE,
  ObITable::TableType::MAJOR_SSTABLE,
  ObITable::TableType::MINOR_SSTABLE,
  ObITable::TableType::MINI_SSTABLE,
  ObITable::TableType::META_MAJOR_SSTABLE,
  ObITable::TableType::DDL_DUMP_SSTABLE,
  ObITable::TableType::REMOTE_LOGICAL_MINOR_SSTABLE,
  ObITable::TableType::DDL_MEM_SSTABLE,
  ObITable::TableType::COLUMN_ORIENTED_SSTABLE,
  ObITable::TableType::NORMAL_COLUMN_GROUP_SSTABLE,
  ObITable::TableType::ROWKEY_COLUMN_GROUP_SSTABLE,
  ObITable::TableType::COLUMN_ORIENTED_META_SSTABLE,
  ObITable::TableType::DDL_MERGE_CO_SSTABLE,
  ObITable::TableType::DDL_MERGE_CG_SSTABLE,
  ObITable::TableType::DDL_MEM_CO_SSTABLE,
  ObITable::TableType::DDL_MEM_CG_SSTABLE,
  ObITable::TableType::DDL_MEM_MINI_SSTABLE,
  ObITable::TableType::MICRO_MINI_SSTABLE,
  ObITable::TableType::INC_MAJOR_SSTABLE,
  ObITable::TableType::INC_COLUMN_ORIENTED_SSTABLE,
  ObITable::TableType::INC_NORMAL_COLUMN_GROUP_SSTABLE,
  ObITable::TableType::INC_ROWKEY_COLUMN_GROUP_SSTABLE,
  ObITable::TableType::MAX_TABLE_TYPE
};

// TYPE_MAP indices (not ObITable::TableType enum values). After removing
// MDS_MINI/MDS_MINOR entries, INC_MAJOR moved from index 29 to 27.
static constexpr int64_t TYPE_MAP_IDX_INC_MAJOR = 27;

class TestTableStore : public ::testing::Test
{
public:
  TestTableStore();
  virtual ~TestTableStore();
  virtual void SetUp();
  virtual void TearDown();
  int mock_sstable(const ObITable::TableKey &key, ObSSTable *&sstable, int64_t tx_id = 100, int64_t seq_no = 200);
  int mock_memtable(const ObITable::TableKey &key, ObMemtable *&memtable);
  int batch_mock_tables();
  int mock_tablet(ObTablet *&tablet);
  int mock_old_table_store(ObTabletTableStore *table_store);
private:
  ObArenaAllocator allocator_;
  ObMockIterator data_iter_;
  ObTabletID tablet_id_;
  const char *key_data_;
  ObArray<ObITable *> sstables_;
  ObArray<ObMemtable *> memtables_;
};

TestTableStore::TestTableStore()
  : allocator_(),
    data_iter_(),
    tablet_id_(200001),
    key_data_(nullptr),
    sstables_(),
    memtables_()
{
}

TestTableStore::~TestTableStore()
{
  allocator_.reset();
  data_iter_.reset();
  sstables_.reset();
  memtables_.reset();
  key_data_ = nullptr;
}

void TestTableStore::SetUp()
{
}

void TestTableStore::TearDown()
{
}

int TestTableStore::mock_sstable(
    const ObITable::TableKey &key,
    ObSSTable *&sstable,
    int64_t tx_id,
    int64_t seq_no)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  sstable = nullptr;

  ObTabletCreateSSTableParam param;
  param.table_key_ = key;
  param.max_merged_trans_version_ = key.scn_range_.end_scn_.get_val_for_tx();
  param.schema_version_ = 100;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 0;
  param.progressive_merge_step_ = 0;
  param.index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
  param.table_mode_.mode_flag_ = ObTableModeFlag::TABLE_MODE_NORMAL;
  param.table_mode_.pk_mode_ = ObTablePKMode::TPKM_OLD_NO_PK;
  param.table_mode_.state_flag_ = ObTableStateFlag::TABLE_STATE_NORMAL;
  param.rowkey_column_cnt_ = 3;
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = 0;
  param.micro_block_cnt_ = 0;
  param.use_old_macro_block_count_ = 0;
  param.data_checksum_ = 0;
  param.occupy_size_ = 0;
  param.ddl_scn_.set_min();
  param.filled_tx_scn_ = key.scn_range_.end_scn_;
  param.tx_data_recycle_scn_.set_min();
  param.rec_scn_.set_min();
  param.original_size_ = 0;
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.table_backup_flag_.reset();
  param.table_shared_flag_.reset();
  param.sstable_logic_seq_ = 0;
  param.row_count_ = 0;
  param.recycle_version_ = 0;
  param.nested_size_ = 0;
  param.nested_offset_ = 0;
  param.column_group_cnt_ = 1;
  param.co_base_type_ = ObCOSSTableBaseType::INVALID_TYPE;
  param.full_column_cnt_ = 0;
  param.is_co_table_without_cgs_ = false;
  param.co_base_snapshot_version_ = 0;
  param.column_cnt_ = param.rowkey_column_cnt_;
  ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_);

  if (key.is_major_sstable()) {
    param.rec_scn_.set_min();
  } else {
    param.rec_scn_ = SCN::plus(key.get_start_scn(), 1);
  }

  if (ObITable::is_inc_major_type_sstable(key.table_type_) || ObITable::is_inc_major_ddl_sstable(key.table_type_)) {
    compaction::ObUncommitTxDesc tx_desc(tx_id, ObTxSEQ(seq_no, 0).cast_to_int());
    param.uncommit_tx_info_.push_back(tx_desc);
    param.uncommit_tx_info_.set_info_status_record_seq();
    param.ddl_scn_.set_base();
  }


  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSTable)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for sstable", K(ret));
  } else if (OB_ISNULL(sstable = new (buf) ObSSTable())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for sstable", K(ret));
  } else if (OB_FAIL(sstable->init(param, &allocator_))) {
    LOG_WARN("failed to init sstable", K(ret));
  } else {
    if (sstable->is_meta_major_sstable()) {
      sstable->meta_cache_.max_merged_trans_version_ = key.scn_range_.end_scn_.get_val_for_tx();
      sstable->meta_->basic_meta_.max_merged_trans_version_ = key.scn_range_.end_scn_.get_val_for_tx();
    } else if (sstable->is_inc_major_type_sstable() || sstable->is_inc_major_ddl_sstable()) {
      sstable->meta_->basic_meta_.data_macro_block_count_ = 1;
      sstable->meta_cache_.data_macro_block_count_ = 1;
    }
  }
  return ret;
}

int TestTableStore::mock_memtable(const ObITable::TableKey &key, ObMemtable *&memtable)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  memtable = nullptr;

  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObMemtable)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for memtable", K(ret));
  } else if (OB_ISNULL(memtable = new (buf) ObMemtable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to allocate memory for memtable", K(ret));
  } else {
    memtable->key_ = key;
    memtable->is_inited_ = true;
  }
  return ret;
}

int TestTableStore::batch_mock_tables()
{
  int ret = OB_SUCCESS;
  sstables_.reset();
  memtables_.reset();

  if (nullptr != key_data_) {
    data_iter_.from(key_data_);
    for (int64_t idx = 0; idx < data_iter_.count(); ++idx) {
      const ObStoreRow *row = nullptr;
      data_iter_.get_row(idx, row);
      const ObObj *cells = row->row_val_.cells_;

      ObITable::TableKey table_key;
      table_key.tablet_id_ = tablet_id_;
      table_key.table_type_ = TYPE_MAP[cells[0].get_int()];
      table_key.scn_range_.start_scn_.convert_for_tx(cells[1].get_int());
      table_key.scn_range_.end_scn_.convert_for_tx(cells[2].get_int());

      if (ObITable::is_sstable(table_key.table_type_)) {
        ObSSTable *sstable = nullptr;
        if (OB_FAIL(mock_sstable(table_key, sstable))) {
          LOG_WARN("failed to mock sstable", K(ret), K(table_key));
        } else if (FALSE_IT(sstable->meta_->basic_meta_.max_merged_trans_version_ = cells[3].get_int())) {
        } else if (FALSE_IT(sstable->meta_->basic_meta_.upper_trans_version_ = 99999 == cells[4].get_int()
                                                                            ? INT64_MAX
                                                                            : cells[4].get_int())) {
        } else if (FALSE_IT(sstable->meta_cache_.max_merged_trans_version_ = cells[3].get_int())) {
        } else if (FALSE_IT(sstable->meta_cache_.upper_trans_version_ = sstable->meta_->basic_meta_.upper_trans_version_)) {
        } else if (OB_FAIL(sstables_.push_back(sstable))) {
          LOG_WARN("failed to add sstable", K(ret), KPC(sstable));
        }
      } else {
        ObMemtable *memtable = nullptr;
        if (OB_FAIL(mock_memtable(table_key, memtable))) {
          LOG_WARN("failed to mock sstable", K(ret), K(table_key));
        } else if (FALSE_IT(memtable->snapshot_version_.convert_for_tx(cells[3].get_int()))) {
        } else if (OB_FAIL(memtables_.push_back(memtable))) {
          LOG_WARN("failed to add memtable", K(ret), KPC(memtable));
        }
      }
    }
  }
  return ret;
}

int TestTableStore::mock_old_table_store(ObTabletTableStore *table_store)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(batch_mock_tables())) {
    LOG_WARN("failed to mock tables", K(ret));
  } else if (!sstables_.empty()) {
    ObArray<ObMetaDiskAddr> addrs;
    ObMetaDiskAddr addr;
    addr.set_mem_addr(1, 1);

    ObMajorChecksumInfo ckm_info;
    for (int64_t i = 0; i < sstables_.count(); ++i) {
      if (OB_FAIL(addrs.push_back(addr))) {
        LOG_WARN("failed to add addr", K(ret));
      }
    }

    if (FAILEDx(table_store->init(allocator_, sstables_, addrs, ckm_info))) {
      LOG_WARN("failed to init table store", K(ret));
    }
  }
  return ret;
}

int TestTableStore::mock_tablet(ObTablet *&tablet)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  tablet = nullptr;

  if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObTablet)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for tablet", K(ret));
  } else if (OB_ISNULL(tablet = new (buf) ObTablet())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Fail to allocate memory for tablet", K(ret));
  } else {
    tablet->is_inited_ = true;
  }
  return ret;
}


TEST_F(TestTableStore, test_tablet_report_without_freeze_info)
{
  ServerModeGuard mode_guard(share::ObServerMode::SHARED_STORAGE_MODE);
  const uint64_t tenant_id = 1002;
  const ObLSID ls_id(1001);
  const int64_t snapshot_version = 100;
  const int64_t row_count = 23;
  const int64_t data_checksum = 12345;
  const bool need_checksums = true;
  const int64_t column_checksums[] = {11, 22, 33};
  lib::ObMallocAllocator *malloc_allocator = lib::ObMallocAllocator::get_instance();
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->create_and_add_tenant_allocator(tenant_id));
  DEFER(malloc_allocator->recycle_tenant_allocator(tenant_id));
  ASSERT_EQ(OB_SUCCESS, malloc_allocator->set_tenant_limit(tenant_id, 1LL << 30));
  ObTenantBase tenant_base(tenant_id);
  ObTenantFreezeInfoMgr freeze_info_mgr;
  ObMySQLProxy sql_proxy;
  ASSERT_EQ(OB_SUCCESS, freeze_info_mgr.freeze_info_mgr_.init(tenant_id, sql_proxy));
  freeze_info_mgr.init_for_test();
  tenant_base.set(&freeze_info_mgr);
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(&tenant_base));

  ObFreezeInfo freeze_info;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, freeze_info_mgr.get_lower_bound_freeze_info_before_snapshot_version(
      snapshot_version, freeze_info));

  ObITable::TableKey key;
  key.tablet_id_ = tablet_id_;
  key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  key.scn_range_.start_scn_.set_min();
  ASSERT_EQ(OB_SUCCESS, key.scn_range_.end_scn_.convert_for_tx(snapshot_version));
  ObSSTable *sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
  ASSERT_EQ(snapshot_version, sstable->get_snapshot_version());
  ASSERT_EQ(ARRAYSIZEOF(column_checksums), sstable->meta_->get_col_checksum_cnt());
  for (int64_t i = 0; i < ARRAYSIZEOF(column_checksums); ++i) {
    sstable->meta_->get_col_checksum()[i] = column_checksums[i];
  }
  ObTabletTableStore table_store;
  ASSERT_EQ(OB_SUCCESS, table_store.major_tables_.init(allocator_, sstable));
  sstable->~ObSSTable();

  ObTablet tablet;
  tablet.tablet_meta_.tablet_id_ = tablet_id_;
  tablet.tablet_meta_.ls_id_ = ls_id;
  tablet.tablet_meta_.ddl_snapshot_version_ = 0;
  tablet.tablet_meta_.report_status_.merge_snapshot_version_ = snapshot_version;
  tablet.tablet_meta_.report_status_.row_count_ = row_count;
  tablet.tablet_meta_.report_status_.data_checksum_ = data_checksum;
  ObAddr addr;
  ASSERT_TRUE(addr.set_ip_addr("127.0.0.1", 2882));
  const struct {
    bool is_cs_replica_;
    ObDataChecksumType checksum_type_;
  } cases[] = {
    {false, ObDataChecksumType::DATA_CHECKSUM_NORMAL_WITH_NORMAL_COLUMN},
    {true, ObDataChecksumType::DATA_CHECKSUM_COLUMN_STORE_WITH_NORMAL_COLUMN}
  };
  for (const auto &test_case : cases) {
    SCOPED_TRACE(test_case.is_cs_replica_);
    ObTabletReplica replica;
    ObTabletReplicaChecksumItem checksum;
    ASSERT_EQ(OB_SUCCESS, tablet.get_tablet_report_info_by_sstable(
        addr, table_store, replica, checksum, need_checksums, test_case.is_cs_replica_));
    ASSERT_EQ(tenant_id, replica.get_tenant_id());
    ASSERT_EQ(ls_id, replica.get_ls_id());
    ASSERT_EQ(tablet_id_, replica.get_tablet_id());
    ASSERT_EQ(snapshot_version, replica.get_snapshot_version());
    ASSERT_EQ(tenant_id, checksum.tenant_id_);
    ASSERT_EQ(ls_id, checksum.ls_id_);
    ASSERT_EQ(tablet_id_, checksum.tablet_id_);
    ASSERT_EQ(addr, checksum.server_);
    ASSERT_EQ(snapshot_version, checksum.compaction_scn_.get_val_for_tx());
    ASSERT_EQ(row_count, checksum.row_count_);
    ASSERT_EQ(data_checksum, checksum.data_checksum_);
    ASSERT_EQ(test_case.checksum_type_, checksum.data_checksum_type_);
    ASSERT_EQ(ARRAYSIZEOF(column_checksums), checksum.column_meta_.column_checksums_.count());

    ObObj obj;
    ObString serialized;
    ObTabletReplicaReportColumnMeta decoded;
    ASSERT_EQ(OB_SUCCESS, checksum.column_meta_.get_str_obj(
        checksum.data_checksum_type_, allocator_, obj, serialized));
    ASSERT_EQ(OB_SUCCESS, decoded.set_with_str(checksum.data_checksum_type_, serialized));
    ASSERT_EQ(ARRAYSIZEOF(column_checksums), decoded.column_checksums_.count());
    for (int64_t i = 0; i < ARRAYSIZEOF(column_checksums); ++i) {
      ASSERT_EQ(column_checksums[i], checksum.column_meta_.column_checksums_.at(i));
      ASSERT_EQ(column_checksums[i], decoded.column_checksums_.at(i));
    }
  }
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, freeze_info_mgr.get_lower_bound_freeze_info_before_snapshot_version(
      snapshot_version, freeze_info));
}


TEST_F(TestTableStore, test_basic)
{
  key_data_ = nullptr;
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            1          1          1        \n"
              "11            1            150        150        150      \n"
              "11            150          200        200        200      \n"
              "0             200          250        250        250      \n"
              "0             250          300        300        300      \n"
              "0             300          350        350        350      \n";
  ObTabletTableStore table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&table_store));
  EXPECT_EQ(1, table_store.major_tables_.count());
  EXPECT_EQ(2, table_store.minor_tables_.count());
}


TEST_F(TestTableStore, test_first_init_table_store)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));

  ObITable::TableKey major_key;
  major_key.tablet_id_ = tablet_id_;
  major_key.scn_range_.start_scn_.set_min();
  major_key.scn_range_.end_scn_.set_base();
  major_key.table_type_ = ObITable::TableType::META_MAJOR_SSTABLE;

  ObSSTable *new_major = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(major_key, new_major));

  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_INVALID_ARGUMENT, new_table_store.init(allocator_, *tablet, new_major));

  new_major->key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, new_major));
}


TEST_F(TestTableStore, test_basic_add_new_major)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            1          1          1        \n"
              "11            1            150        150        150      \n"
              "11            150          200        200        200      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.minor_tables_.count());


  ObSSTable *new_major = nullptr;
  ObITable::TableKey major_key;
  major_key.tablet_id_ = tablet_id_;
  major_key.scn_range_.start_scn_.set_min();
  major_key.scn_range_.end_scn_.convert_for_tx(200);
  major_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(major_key, new_major));


  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 150;
  param.sstable_ = new_major;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MAJOR_MERGE;
  param.compaction_info_.need_report_ = true;
  ObStorageSchema storage_schema;
  param.storage_schema_ = &storage_schema;


  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(2, new_table_store.major_tables_.count());
  EXPECT_EQ(2, new_table_store.minor_tables_.count());


  new_table_store.reset();
  param.multi_version_start_ = 200;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(0, new_table_store.minor_tables_.count());
}


TEST_F(TestTableStore, test_basic_add_new_minor)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(300);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            1          1          1        \n"
              "11            100          150        150        150      \n"
              "11            150          200        200        200      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.minor_tables_.count());


  ObSSTable *new_minor = nullptr;
  ObITable::TableKey minor_key;
  minor_key.tablet_id_ = tablet_id_;
  minor_key.scn_range_.start_scn_.convert_for_tx(200);
  minor_key.scn_range_.end_scn_.convert_for_tx(300);
  minor_key.table_type_ = ObITable::TableType::MINI_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(minor_key, new_minor));


  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 50;
  param.sstable_ = new_minor;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MINI_MERGE;
  param.compaction_info_.need_report_ = false;


  // test add new mini sstable after mini merge
  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(3, new_table_store.minor_tables_.count());

  // test reject an obsolete mini sstable whose end scn is behind the clog checkpoint
  new_table_store.reset();
  new_minor->key_.scn_range_.end_scn_.convert_for_tx(250);
  EXPECT_EQ(OB_NO_NEED_MERGE, new_table_store.init(allocator_, *tablet, param, old_table_store));
  new_minor->key_.scn_range_.end_scn_.convert_for_tx(300);


  // test add a mini sstable which end scn not equals to the clog checkpoint scn
  new_table_store.reset();
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  EXPECT_EQ(OB_ERR_UNEXPECTED, new_table_store.init(allocator_, *tablet, param, old_table_store));


  // test add a new minor which cover all mini sstables in old store
  new_table_store.reset();
  new_minor->key_.scn_range_.start_scn_.convert_for_tx(100);
  new_minor->key_.scn_range_.end_scn_.convert_for_tx(200);
  new_minor->meta_cache_.upper_trans_version_ = 200;
  new_minor->meta_->basic_meta_.upper_trans_version_ = 200;
  param.compaction_info_.merge_type_ = ObMergeType::MINOR_MERGE;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(1, new_table_store.minor_tables_.count());


  // test add a range crossed sstable
  new_table_store.reset();
  new_minor->key_.scn_range_.start_scn_.convert_for_tx(50);
  new_minor->key_.scn_range_.end_scn_.convert_for_tx(180);
  EXPECT_EQ(OB_MINOR_SSTABLE_RANGE_CROSS, new_table_store.init(allocator_, *tablet, param, old_table_store));


  // test add a new table which not cover any mini sstable in minor merge
  new_table_store.reset();
  new_minor->key_.scn_range_.start_scn_.convert_for_tx(50);
  new_minor->key_.scn_range_.end_scn_.convert_for_tx(60);
  EXPECT_EQ(OB_NO_NEED_MERGE, new_table_store.init(allocator_, *tablet, param, old_table_store));

  new_table_store.reset();
  new_minor->key_.scn_range_.start_scn_.convert_for_tx(250);
  new_minor->key_.scn_range_.end_scn_.convert_for_tx(300);
  EXPECT_EQ(OB_NO_NEED_MERGE, new_table_store.init(allocator_, *tablet, param, old_table_store));
}


TEST_F(TestTableStore, test_basic_add_new_inc_major)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            50         50         50       \n"
              "11            50           150        150        150      \n"
              "11            150          200        200        200      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.minor_tables_.count());


  ObSSTable *new_inc_major = nullptr;
  ObITable::TableKey major_key;
  major_key.tablet_id_ = tablet_id_;
  major_key.scn_range_.start_scn_.set_min();
  major_key.scn_range_.end_scn_.convert_for_tx(250);
  major_key.table_type_ = ObITable::TableType::INC_MAJOR_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(major_key, new_inc_major));
  new_inc_major->meta_->basic_meta_.upper_trans_version_ = 250;
  new_inc_major->meta_cache_.upper_trans_version_ = 250;


  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 150;
  param.sstable_ = new_inc_major;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MAJOR_MERGE;
  param.compaction_info_.need_report_ = true;


  // test add a new committed inc major
  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(2, new_table_store.minor_tables_.count());
  EXPECT_EQ(1, new_table_store.inc_major_tables_.count());


  // test add a aborted inc major
  new_table_store.reset();
  new_inc_major->meta_->basic_meta_.upper_trans_version_ = -1;
  new_inc_major->meta_cache_.upper_trans_version_ = -1;
  EXPECT_EQ(OB_NO_NEED_MERGE, new_table_store.init(allocator_, *tablet, param, old_table_store));
}


TEST_F(TestTableStore, test_same_key_inc_major_for_ss)
{
  ObTablet *tablet = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n"
              "27            0            150        150        99999    \n";
  ObTabletTableStore old_table_store;
  ASSERT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  ASSERT_EQ(1, old_table_store.inc_major_tables_.count());
  ObSSTable *old_inc_major = old_table_store.inc_major_tables_[0];
  ASSERT_NE(nullptr, old_inc_major);
  ASSERT_EQ(INT64_MAX, old_inc_major->get_upper_trans_version());

  const ObITable::TableKey same_key = old_inc_major->get_key();
  ObSSTable *duplicate_add_inc_major = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(same_key, duplicate_add_inc_major, 100, 200));
  duplicate_add_inc_major->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  duplicate_add_inc_major->meta_cache_.upper_trans_version_ = INT64_MAX;
  duplicate_add_inc_major->meta_->basic_meta_.data_checksum_ = 111;
  duplicate_add_inc_major->meta_cache_.data_checksum_ = 111;

  ObSSTable *committed_inc_major = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(same_key, committed_inc_major, 100, 200));
  committed_inc_major->meta_->basic_meta_.upper_trans_version_ = 250;
  committed_inc_major->meta_cache_.upper_trans_version_ = 250;
  committed_inc_major->meta_->basic_meta_.data_checksum_ = 222;
  committed_inc_major->meta_cache_.data_checksum_ = 222;

  ObSSTable *compaction_inc_major = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      mock_sstable(same_key, compaction_inc_major, 300, 400));
  compaction_inc_major->meta_->basic_meta_.upper_trans_version_ = 250;
  compaction_inc_major->meta_cache_.upper_trans_version_ = 250;
  compaction_inc_major->meta_->basic_meta_.data_checksum_ = 333;
  compaction_inc_major->meta_cache_.data_checksum_ = 333;

  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 100;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MEDIUM_MERGE;
  param.compaction_info_.need_report_ = false;
  ServerModeGuard mode_guard(share::ObServerMode::SHARED_STORAGE_MODE);

  // Table Store 确认目标已存在后，将重复 ADD 返回给上层处理。
  param.sstable_ = duplicate_add_inc_major;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::ADD;
  ObTabletTableStore duplicate_add_table_store;
  EXPECT_EQ(OB_SSTABLE_EXIST,
      duplicate_add_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_FALSE(duplicate_add_table_store.is_valid());

  // COMMIT 必须携带有限 upper_trans_version。
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::COMMIT;
  ObTabletTableStore invalid_commit_table_store;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      invalid_commit_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_FALSE(invalid_commit_table_store.is_valid());

  // COMMIT 应用有限 upper_trans_version 的新元数据替换同身份未提交表。
  param.sstable_ = committed_inc_major;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::COMMIT;
  ObTabletTableStore committed_table_store;
  EXPECT_EQ(OB_SUCCESS,
      committed_table_store.init(allocator_, *tablet, param, old_table_store));
  ASSERT_EQ(1, committed_table_store.inc_major_tables_.count());
  EXPECT_EQ(222,
      committed_table_store.inc_major_tables_[0]->meta_->basic_meta_.data_checksum_);
  EXPECT_EQ(250, committed_table_store.inc_major_tables_[0]->get_upper_trans_version());

  // 只有目标提交版本相同时，才将重复 COMMIT 返回给上层处理。
  param.sstable_ = committed_inc_major;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::COMMIT;
  ObTabletTableStore duplicate_commit_table_store;
  EXPECT_EQ(OB_SSTABLE_EXIST,
      duplicate_commit_table_store.init(allocator_, *tablet, param, committed_table_store));
  EXPECT_FALSE(duplicate_commit_table_store.is_valid());

  // ABORT 不能删除已经提交的 Inc Major。
  param.sstable_ = duplicate_add_inc_major;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::ABORT;
  ObTabletTableStore invalid_abort_table_store;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      invalid_abort_table_store.init(allocator_, *tablet, param, committed_table_store));
  EXPECT_FALSE(invalid_abort_table_store.is_valid());

  // Inc Major Compaction 必须基于当前 Table Store 中仍然存在且 generation 未变化的输入。
  // Compaction 可替换 same-key 输入，其代表事务身份不要求与被覆盖表一致。
  param.sstable_ = compaction_inc_major;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::COMPACTION;
  param.compaction_info_.merge_type_ = ObMergeType::INC_MAJOR_MERGE;

  ObTablesHandleArray stale_generation_inputs;
  ASSERT_EQ(OB_SUCCESS,
      stale_generation_inputs.add_sstable(old_inc_major, ObStorageMetaHandle()));
  param.input_tables_handle_ = &stale_generation_inputs;
  ObTabletTableStore stale_generation_table_store;
  EXPECT_EQ(OB_NO_NEED_MERGE,
      stale_generation_table_store.init(allocator_, *tablet, param, committed_table_store));
  EXPECT_FALSE(stale_generation_table_store.is_valid());

  ObTablesHandleArray different_tx_inputs;
  ASSERT_EQ(OB_SUCCESS,
      different_tx_inputs.add_sstable(compaction_inc_major, ObStorageMetaHandle()));
  param.input_tables_handle_ = &different_tx_inputs;
  ObTabletTableStore different_tx_compaction_table_store;
  EXPECT_EQ(OB_NO_NEED_MERGE,
      different_tx_compaction_table_store.init(allocator_, *tablet, param, committed_table_store));
  EXPECT_FALSE(different_tx_compaction_table_store.is_valid());

  ObTablesHandleArray compaction_inputs;
  ASSERT_EQ(OB_SUCCESS, compaction_inputs.add_sstable(
      committed_table_store.inc_major_tables_[0], ObStorageMetaHandle()));
  param.input_tables_handle_ = &compaction_inputs;
  ObTabletTableStore compaction_table_store;
  EXPECT_EQ(OB_SUCCESS,
      compaction_table_store.init(allocator_, *tablet, param, committed_table_store));
  ASSERT_EQ(1, compaction_table_store.inc_major_tables_.count());
  EXPECT_EQ(333,
      compaction_table_store.inc_major_tables_[0]->meta_->basic_meta_.data_checksum_);
  EXPECT_EQ(250,
      compaction_table_store.inc_major_tables_[0]->get_upper_trans_version());

  // Local Link 安装 Shared 侧权威结果，不要求携带本地 Compaction 输入。
  param.input_tables_handle_ = nullptr;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::LINK;
  ObTabletTableStore linked_table_store;
  EXPECT_EQ(OB_SUCCESS,
      linked_table_store.init(allocator_, *tablet, param, committed_table_store));
  ASSERT_EQ(1, linked_table_store.inc_major_tables_.count());
  EXPECT_EQ(333,
      linked_table_store.inc_major_tables_[0]->meta_->basic_meta_.data_checksum_);
  EXPECT_EQ(250,
      linked_table_store.inc_major_tables_[0]->get_upper_trans_version());

  // ABORT 只删除同身份的未提交 Inc Major，不影响 Tablet 中的 Major 表。
  param.sstable_ = old_inc_major;
  param.compaction_info_.merge_type_ = ObMergeType::MEDIUM_MERGE;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::ABORT;
  ObTabletTableStore aborted_table_store;
  EXPECT_EQ(OB_SUCCESS,
      aborted_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(0, aborted_table_store.inc_major_tables_.count());
  EXPECT_EQ(1, aborted_table_store.major_tables_.count());

  // COMMIT 是严格更新，目标不存在时不能将待提交表重新加入 Table Store。
  param.sstable_ = committed_inc_major;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::COMMIT;
  ObTabletTableStore missing_commit_table_store;
  EXPECT_EQ(OB_SSTABLE_NOT_EXIST,
      missing_commit_table_store.init(allocator_, *tablet, param, aborted_table_store));
  EXPECT_FALSE(missing_commit_table_store.is_valid());

  // 普通 Link 允许安装 Local 侧不存在的 Shared 权威结果。
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::LINK;
  ObTabletTableStore linked_missing_table_store;
  EXPECT_EQ(OB_SUCCESS,
      linked_missing_table_store.init(allocator_, *tablet, param, aborted_table_store));
  ASSERT_EQ(1, linked_missing_table_store.inc_major_tables_.count());
  EXPECT_EQ(222,
      linked_missing_table_store.inc_major_tables_[0]->meta_->basic_meta_.data_checksum_);
  EXPECT_EQ(250,
      linked_missing_table_store.inc_major_tables_[0]->get_upper_trans_version());

  // 普通 Link 同样允许安装 Shared 侧尚未提交的 Inc Major。
  param.sstable_ = duplicate_add_inc_major;
  ObTabletTableStore linked_missing_uncommitted_table_store;
  EXPECT_EQ(OB_SUCCESS,
      linked_missing_uncommitted_table_store.init(allocator_, *tablet, param, aborted_table_store));
  ASSERT_EQ(1, linked_missing_uncommitted_table_store.inc_major_tables_.count());
  EXPECT_EQ(INT64_MAX,
      linked_missing_uncommitted_table_store.inc_major_tables_[0]->get_upper_trans_version());

}


#ifdef OB_BUILD_SHARED_STORAGE
TEST_F(TestTableStore, test_async_index_inc_major_retry_keeps_same_logical_window)
{
  ObTablet *tablet = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->table_store_cache_.is_async_index_refresh_target_ = true;

  ObITable::TableKey first_key;
  first_key.tablet_id_ = tablet_id_;
  first_key.table_type_ = ObITable::INC_MAJOR_SSTABLE;
  ASSERT_EQ(OB_SUCCESS, first_key.scn_range_.start_scn_.convert_for_tx(1000));
  ASSERT_EQ(OB_SUCCESS, first_key.scn_range_.end_scn_.convert_for_tx(1100));
  ObITable::TableKey second_key = first_key;
  ASSERT_EQ(OB_SUCCESS, second_key.scn_range_.start_scn_.convert_for_tx(1100));
  ASSERT_EQ(OB_SUCCESS, second_key.scn_range_.end_scn_.convert_for_tx(1200));

  ObSSTable *first_inc_major = nullptr;
  ObSSTable *second_inc_major = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(first_key, first_inc_major, 100, 200));
  ASSERT_EQ(OB_SUCCESS, mock_sstable(second_key, second_inc_major, 300, 400));
  first_inc_major->meta_->basic_meta_.min_merged_trans_version_ = 1;
  first_inc_major->meta_->basic_meta_.max_merged_trans_version_ = 150;
  first_inc_major->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  first_inc_major->meta_cache_.min_merged_trans_version_ = 1;
  first_inc_major->meta_cache_.max_merged_trans_version_ = 150;
  first_inc_major->meta_cache_.upper_trans_version_ = INT64_MAX;
  second_inc_major->meta_->basic_meta_.min_merged_trans_version_ = 1;
  second_inc_major->meta_->basic_meta_.max_merged_trans_version_ = 150;
  second_inc_major->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  second_inc_major->meta_cache_.min_merged_trans_version_ = 1;
  second_inc_major->meta_cache_.max_merged_trans_version_ = 150;
  second_inc_major->meta_cache_.upper_trans_version_ = INT64_MAX;

  ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> old_inc_major_tables;
  ASSERT_EQ(OB_SUCCESS, old_inc_major_tables.push_back(first_inc_major));
  ObTabletTableStore old_table_store;
  ASSERT_EQ(OB_SUCCESS,
      old_table_store.inc_major_tables_.init(allocator_, old_inc_major_tables));

  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 0;
  param.sstable_ = second_inc_major;
  param.compaction_info_.merge_type_ = ObMergeType::MEDIUM_MERGE;
  param.compaction_info_.need_report_ = false;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::ADD;
  ObStorageSchema storage_schema;
  storage_schema.index_type_ = INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL;
  param.storage_schema_ = &storage_schema;
  ServerModeGuard mode_guard(share::ObServerMode::SHARED_STORAGE_MODE);

  ObTabletTableStore added_table_store;
  ASSERT_EQ(OB_SUCCESS, added_table_store.inner_build_inc_major_tables_for_ss(
      allocator_, param, old_table_store, 0));
  ASSERT_EQ(2, added_table_store.inc_major_tables_.count());
  // 构造数组时保留不同物理 key，逻辑区间回收在后续步骤执行。
  EXPECT_TRUE(first_key == added_table_store.inc_major_tables_.at(0)->get_key()
      || first_key == added_table_store.inc_major_tables_.at(1)->get_key());
  EXPECT_TRUE(second_key == added_table_store.inc_major_tables_.at(0)->get_key()
      || second_key == added_table_store.inc_major_tables_.at(1)->get_key());

  // MEDIUM_MERGE + ADD 同样按逻辑区间回收；保留回收前的数组供后续独立场景使用。
  ObTabletTableStore recycled_added_table_store;
  ASSERT_EQ(OB_SUCCESS, recycled_added_table_store.inner_build_inc_major_tables_for_ss(
      allocator_, param, old_table_store, 0));
  ASSERT_EQ(OB_SUCCESS,
      recycled_added_table_store.try_recycle_inc_major_tables_(allocator_, param));
  ASSERT_EQ(1, recycled_added_table_store.inc_major_tables_.count());
  EXPECT_EQ(second_key, recycled_added_table_store.inc_major_tables_.at(0)->get_key());

  // 普通 LINK 回收同区间的旧表，避免随后两张表均提交时区间重叠。
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::LINK;
  ObTabletTableStore linked_table_store;
  ASSERT_EQ(OB_SUCCESS, linked_table_store.inner_build_inc_major_tables_for_ss(
      allocator_, param, old_table_store, 0));
  ASSERT_EQ(OB_SUCCESS,
      linked_table_store.try_recycle_inc_major_tables_(allocator_, param));
  ASSERT_EQ(1, linked_table_store.inc_major_tables_.count());
  EXPECT_EQ(second_key, linked_table_store.inc_major_tables_.at(0)->get_key());

  // 真正的 Inc Major Compaction 结果仍应回收被其逻辑版本窗口覆盖的输入表。
  ObITable::TableKey compaction_key = second_key;
  ASSERT_EQ(OB_SUCCESS, compaction_key.scn_range_.start_scn_.convert_for_tx(1200));
  ASSERT_EQ(OB_SUCCESS, compaction_key.scn_range_.end_scn_.convert_for_tx(1300));
  ObSSTable *compaction_inc_major = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(compaction_key, compaction_inc_major, 500, 600));
  compaction_inc_major->meta_->basic_meta_.min_merged_trans_version_ = 1;
  compaction_inc_major->meta_->basic_meta_.max_merged_trans_version_ = 150;
  compaction_inc_major->meta_->basic_meta_.upper_trans_version_ = 1250;
  compaction_inc_major->meta_cache_.min_merged_trans_version_ = 1;
  compaction_inc_major->meta_cache_.max_merged_trans_version_ = 150;
  compaction_inc_major->meta_cache_.upper_trans_version_ = 1250;

  ObTablesHandleArray compaction_inputs;
  ASSERT_EQ(OB_SUCCESS,
      compaction_inputs.add_sstable(first_inc_major, ObStorageMetaHandle()));
  ASSERT_EQ(OB_SUCCESS,
      compaction_inputs.add_sstable(second_inc_major, ObStorageMetaHandle()));
  param.sstable_ = compaction_inc_major;
  param.input_tables_handle_ = &compaction_inputs;
  param.compaction_info_.merge_type_ = ObMergeType::INC_MAJOR_MERGE;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::COMPACTION;
  ObTabletTableStore compaction_table_store;
  ASSERT_EQ(OB_SUCCESS, compaction_table_store.inner_build_inc_major_tables_for_ss(
      allocator_, param, added_table_store, 0));
  ASSERT_EQ(3, compaction_table_store.inc_major_tables_.count());
  ASSERT_EQ(OB_SUCCESS,
      compaction_table_store.try_recycle_inc_major_tables_(allocator_, param));
  ASSERT_EQ(1, compaction_table_store.inc_major_tables_.count());
  EXPECT_EQ(compaction_key, compaction_table_store.inc_major_tables_.at(0)->get_key());

  // Follower Link 真正的 Compaction 结果时也应执行相同的覆盖回收。
  param.input_tables_handle_ = nullptr;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::LINK;
  ObTabletTableStore linked_compaction_table_store;
  ASSERT_EQ(OB_SUCCESS,
      linked_compaction_table_store.inner_build_inc_major_tables_for_ss(
          allocator_, param, added_table_store, 0));
  ASSERT_EQ(3, linked_compaction_table_store.inc_major_tables_.count());
  ASSERT_EQ(OB_SUCCESS,
      linked_compaction_table_store.try_recycle_inc_major_tables_(allocator_, param));
  ASSERT_EQ(1, linked_compaction_table_store.inc_major_tables_.count());
  EXPECT_EQ(compaction_key,
      linked_compaction_table_store.inc_major_tables_.at(0)->get_key());

  // A failed retry is removed by its own key, leaving the earlier candidate.
  param.sstable_ = second_inc_major;
  param.compaction_info_.merge_type_ = ObMergeType::MEDIUM_MERGE;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::ABORT;
  ObTabletTableStore aborted_retry_store;
  ASSERT_EQ(OB_SUCCESS, aborted_retry_store.inner_build_inc_major_tables_for_ss(
      allocator_, param, added_table_store, 0));
  ASSERT_EQ(1, aborted_retry_store.inc_major_tables_.count());
  EXPECT_EQ(first_key, aborted_retry_store.inc_major_tables_.at(0)->get_key());

  ObSSTable *committed_second_inc_major = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      mock_sstable(second_key, committed_second_inc_major, 300, 400));
  committed_second_inc_major->meta_->basic_meta_.min_merged_trans_version_ = 1;
  committed_second_inc_major->meta_->basic_meta_.max_merged_trans_version_ = 150;
  committed_second_inc_major->meta_->basic_meta_.upper_trans_version_ = 1250;
  committed_second_inc_major->meta_cache_.min_merged_trans_version_ = 1;
  committed_second_inc_major->meta_cache_.max_merged_trans_version_ = 150;
  committed_second_inc_major->meta_cache_.upper_trans_version_ = 1250;
  param.sstable_ = committed_second_inc_major;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::COMMIT;

  ObTabletTableStore committed_table_store;
  ASSERT_EQ(OB_SUCCESS, committed_table_store.inner_build_inc_major_tables_for_ss(
      allocator_, param, added_table_store, 0));
  ASSERT_EQ(2, committed_table_store.inc_major_tables_.count());
  committed_table_store.is_inited_ = true;
  EXPECT_EQ(OB_SUCCESS, committed_table_store.check_continuous(true));

  bool found_first = false;
  bool found_committed_second = false;
  for (int64_t i = 0; i < committed_table_store.inc_major_tables_.count(); ++i) {
    ObSSTable *table = committed_table_store.inc_major_tables_.at(i);
    ASSERT_NE(nullptr, table);
    if (first_key == table->get_key()) {
      found_first = true;
      EXPECT_EQ(INT64_MAX, table->get_upper_trans_version());
    } else if (second_key == table->get_key()) {
      found_committed_second = true;
      EXPECT_EQ(1250, table->get_upper_trans_version());
    }
  }
  EXPECT_TRUE(found_first);
  EXPECT_TRUE(found_committed_second);

  // 普通 LINK 安装更宽的区间时，回收已被覆盖但提交版本尚未回填的旧表。
  ObITable::TableKey covering_key = second_key;
  ASSERT_EQ(OB_SUCCESS, covering_key.scn_range_.start_scn_.convert_for_tx(1300));
  ASSERT_EQ(OB_SUCCESS, covering_key.scn_range_.end_scn_.convert_for_tx(1400));
  ObSSTable *covering_inc_major = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(covering_key, covering_inc_major, 500, 600));
  covering_inc_major->meta_->basic_meta_.min_merged_trans_version_ = 0;
  covering_inc_major->meta_->basic_meta_.max_merged_trans_version_ = 200;
  covering_inc_major->meta_cache_.min_merged_trans_version_ = 0;
  covering_inc_major->meta_cache_.max_merged_trans_version_ = 200;
  covering_inc_major->meta_->basic_meta_.upper_trans_version_ = 1350;
  covering_inc_major->meta_cache_.upper_trans_version_ = 1350;
  param.sstable_ = covering_inc_major;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::LINK;
  ObTabletTableStore committed_covering_link_store;
  ASSERT_EQ(OB_SUCCESS, committed_covering_link_store.inner_build_inc_major_tables_for_ss(
      allocator_, param, committed_table_store, 0));
  ASSERT_EQ(OB_SUCCESS,
      committed_covering_link_store.try_recycle_inc_major_tables_(allocator_, param));
  ASSERT_EQ(1, committed_covering_link_store.inc_major_tables_.count());
  EXPECT_EQ(covering_key, committed_covering_link_store.inc_major_tables_.at(0)->get_key());

  // GC of the aborted first attempt must not remove the committed retry.
  param.sstable_ = first_inc_major;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::ABORT;
  ObTabletTableStore gc_table_store;
  ASSERT_EQ(OB_SUCCESS, gc_table_store.inner_build_inc_major_tables_for_ss(
      allocator_, param, committed_table_store, 0));
  ASSERT_EQ(1, gc_table_store.inc_major_tables_.count());
  ASSERT_NE(nullptr, gc_table_store.inc_major_tables_.at(0));
  EXPECT_EQ(second_key, gc_table_store.inc_major_tables_.at(0)->get_key());
  EXPECT_EQ(1250, gc_table_store.inc_major_tables_.at(0)->get_upper_trans_version());
}
#endif

TEST_F(TestTableStore, test_reject_different_tx_same_key_inc_major_for_ss)
{
  // 不同事务身份生成相同 TableKey 时，应在构造共享 Table Store 时被拦截。
  ObTablet *tablet = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n"
              "27            0            150        150        99999    \n";
  ObTabletTableStore old_table_store;
  ASSERT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  ASSERT_EQ(1, old_table_store.inc_major_tables_.count());
  ObSSTable *old_inc_major = old_table_store.inc_major_tables_[0];
  ASSERT_NE(nullptr, old_inc_major);
  ASSERT_EQ(INT64_MAX, old_inc_major->get_upper_trans_version());

  const ObITable::TableKey same_key = old_inc_major->get_key();
  ObSSTable *different_tx_inc_major = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(same_key, different_tx_inc_major, 300, 400));
  different_tx_inc_major->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  different_tx_inc_major->meta_cache_.upper_trans_version_ = INT64_MAX;
  ASSERT_EQ(INT64_MAX, different_tx_inc_major->get_upper_trans_version());

  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 100;
  param.sstable_ = different_tx_inc_major;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MAJOR_MERGE;
  param.compaction_info_.need_report_ = false;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::ADD;

  ServerModeGuard mode_guard(share::ObServerMode::SHARED_STORAGE_MODE);
  ObTabletTableStore different_tx_table_store;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      different_tx_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_FALSE(different_tx_table_store.is_valid());
  ASSERT_EQ(1, old_table_store.inc_major_tables_.count());
  EXPECT_EQ(old_inc_major, old_table_store.inc_major_tables_[0]);

  // 同一事务但 seq_no 不同时，也必须按不同事务身份拦截。
  ObSSTable *different_seq_inc_major = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(same_key, different_seq_inc_major, 100, 201));
  different_seq_inc_major->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  different_seq_inc_major->meta_cache_.upper_trans_version_ = INT64_MAX;
  param.sstable_ = different_seq_inc_major;
  ObTabletTableStore different_seq_table_store;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      different_seq_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_FALSE(different_seq_table_store.is_valid());
  ASSERT_EQ(1, old_table_store.inc_major_tables_.count());
  EXPECT_EQ(old_inc_major, old_table_store.inc_major_tables_[0]);

  // 普通 Link 仍需校验事务身份，不能用 Shared 侧不同事务的 same-key 表覆盖 Local。
  param.sstable_ = different_tx_inc_major;
  param.inc_major_sstable_action_ = ObIncMajorSSTableAction::LINK;
  param.compaction_info_.merge_type_ = ObMergeType::MEDIUM_MERGE;
  ObTabletTableStore different_tx_link_table_store;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      different_tx_link_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_FALSE(different_tx_link_table_store.is_valid());
  ASSERT_EQ(1, old_table_store.inc_major_tables_.count());
  EXPECT_EQ(old_inc_major, old_table_store.inc_major_tables_[0]);

  ObTransID old_trans_id;
  ObTxSEQ old_seq_no;
  ASSERT_EQ(OB_SUCCESS, ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(
      old_inc_major, old_trans_id, old_seq_no));
  EXPECT_EQ(ObTransID(100), old_trans_id);
  EXPECT_EQ(ObTxSEQ(200, 0), old_seq_no);
  EXPECT_EQ(INT64_MAX, old_inc_major->get_upper_trans_version());
}


TEST_F(TestTableStore, test_replace_table_store)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            50         50         50       \n"
              "11            50           150        150        150      \n"
              "11            150          200        200        200      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.minor_tables_.count());

  // When replace sstable array is null, should assign old store to new store
  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(
      allocator_, *tablet, tablet->is_async_index(), old_table_store, nullptr));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(2, new_table_store.minor_tables_.count());

  // When replace sstable array is null, should assign old store to new store
  ObArray<ObITable *> replace_tables;
  new_table_store.reset();
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(
      allocator_, *tablet, tablet->is_async_index(), old_table_store, &replace_tables));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(2, new_table_store.minor_tables_.count());


  // init table store with valid replaced sstable
  ObITable::TableKey table_key;
  table_key.tablet_id_ = tablet_id_;
  table_key.table_type_ = TYPE_MAP[11];
  table_key.scn_range_.start_scn_.convert_for_tx(50);
  table_key.scn_range_.end_scn_.convert_for_tx(150);
  ObSSTable *sstable = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(table_key, sstable));
  sstable->meta_cache_.nested_offset_ = 2048;
  replace_tables.push_back(sstable);

  new_table_store.reset();
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(
      allocator_, *tablet, tablet->is_async_index(), old_table_store, &replace_tables));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(2, new_table_store.minor_tables_.count());
  EXPECT_EQ(2048, new_table_store.minor_tables_[0]->meta_cache_.nested_offset_);


  // init table store with wrong replaced sstable, should retrun err
  replace_tables.reset();
  sstable->key_.scn_range_.end_scn_.convert_for_tx(180);
  replace_tables.push_back(sstable);

  new_table_store.reset();
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, new_table_store.init(
      allocator_, *tablet, tablet->is_async_index(), old_table_store, &replace_tables));
}


TEST_F(TestTableStore, test_add_meta_table)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());

  ObITable::TableKey table_key;
  table_key.tablet_id_ = tablet_id_;
  table_key.table_type_ = TYPE_MAP[13];
  table_key.scn_range_.start_scn_.convert_for_tx(0);
  table_key.scn_range_.end_scn_.convert_for_tx(150);
  ObSSTable *sstable = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(table_key, sstable));
  sstable->meta_cache_.max_merged_trans_version_ = 150;

  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 50;
  param.sstable_ = sstable;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::META_MAJOR_MERGE;
  param.compaction_info_.need_report_ = false;


  // add new meta major
  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(1, new_table_store.meta_major_tables_.count());


  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "13            0            150        150        150      \n"
              "10            0            100        100        100      \n";
  old_table_store.reset();
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(1, old_table_store.meta_major_tables_.count());


  // update with new meta major
  sstable->key_.scn_range_.end_scn_.convert_for_tx(200);
  new_table_store.reset();
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(1, new_table_store.meta_major_tables_.count());
  EXPECT_EQ(200, new_table_store.meta_major_tables_[0]->key_.scn_range_.end_scn_.get_val_for_tx());


  // update with wrong meta major, expect fail
  sstable->key_.scn_range_.end_scn_.convert_for_tx(50);
  new_table_store.reset();
  EXPECT_EQ(OB_MINOR_SSTABLE_RANGE_CROSS, new_table_store.init(allocator_, *tablet, param, old_table_store));


  // update with a old major, should not recycle meta major
  sstable->key_.table_type_ = ObITable::MAJOR_SSTABLE;
  sstable->key_.scn_range_.end_scn_.convert_for_tx(120);
  param.compaction_info_.merge_type_ = ObMergeType::MAJOR_MERGE;
  new_table_store.reset();
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.meta_major_tables_.count());
}


TEST_F(TestTableStore, test_ha_build_inc_major_tables)
{
  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n"
              "27            0            80         80         -1       \n"
              "27            0            130        130        -1       \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.inc_major_tables_.count());


  ObBatchUpdateTableStoreParam param;
  ObTablesHandleArray source_tables;
  share::SCN source_shared_meta_scn;
  ASSERT_EQ(OB_SUCCESS, source_shared_meta_scn.convert_for_tx(60));
  ObITable::TableKey table_key;
  table_key.tablet_id_ = tablet_id_;
  table_key.table_type_ = ObITable::TableType::INC_MAJOR_SSTABLE;
  table_key.scn_range_.start_scn_.convert_for_tx(0);
  table_key.scn_range_.end_scn_.convert_for_tx(80);

  ObSSTable *sstable_1 = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(table_key, sstable_1));
  sstable_1->meta_cache_.upper_trans_version_ = 90;
  sstable_1->meta_->basic_meta_.upper_trans_version_ = 90;
  ObTableHandleV2 table_hdl_1;
  table_hdl_1.set_sstable(sstable_1, &allocator_);
  source_tables.add_table(table_hdl_1);

  table_key.scn_range_.end_scn_.convert_for_tx(130);
  ObSSTable *sstable_2 = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(table_key, sstable_2));
  sstable_2->meta_cache_.upper_trans_version_ = 150;
  sstable_2->meta_->basic_meta_.upper_trans_version_ = 150;
  ObTableHandleV2 table_hdl_2;
  table_hdl_2.set_sstable(sstable_2, &allocator_);
  source_tables.add_table(table_hdl_2);
  ASSERT_EQ(OB_SUCCESS, ObTableStoreUtil::set_shared_meta_scn_for_inc_major_tables(
      source_tables, source_shared_meta_scn, allocator_, param.tables_handle_));


  // replace two inc majors with commited version ones
  ObTabletTableStore new_table_store;
  ObTablet tablet;
  EXPECT_EQ(OB_SUCCESS, new_table_store.build_ha_inc_major_tables_(
      allocator_, tablet.is_async_index(), param, old_table_store, 100));
  EXPECT_EQ(1, new_table_store.inc_major_tables_.count());
  EXPECT_EQ(150, new_table_store.inc_major_tables_[0]->get_upper_trans_version());
  ObSSTableMetaHandle meta_handle;
  ASSERT_EQ(OB_SUCCESS, new_table_store.inc_major_tables_[0]->get_meta(meta_handle));
  EXPECT_EQ(source_shared_meta_scn,
      meta_handle.get_sstable_meta().get_basic_meta().get_shared_tablet_meta_scn());

  new_table_store.is_inited_ = true;
  tablet.table_store_addr_.ptr_ = &new_table_store;
  ASSERT_EQ(OB_SUCCESS, tablet.init_aggregated_info(allocator_, nullptr));
  EXPECT_EQ(source_shared_meta_scn, tablet.tablet_meta_.min_inc_major_meta_scn_);

  tablet.table_store_addr_.ptr_ = nullptr;
}

TEST_F(TestTableStore, test_ha_async_index_inc_major_preserves_physical_keys)
{
  const int64_t logical_base = 100;
  const int64_t logical_snapshot = 150;
  ObITable::TableKey incoming_key;
  incoming_key.tablet_id_ = tablet_id_;
  incoming_key.table_type_ = ObITable::TableType::INC_MAJOR_SSTABLE;
  ASSERT_EQ(OB_SUCCESS, incoming_key.scn_range_.start_scn_.convert_for_tx(900));
  ASSERT_EQ(OB_SUCCESS, incoming_key.scn_range_.end_scn_.convert_for_tx(980));

  ObITable::TableKey old_key = incoming_key;
  ASSERT_EQ(OB_SUCCESS, old_key.scn_range_.start_scn_.convert_for_tx(800));
  ASSERT_EQ(OB_SUCCESS, old_key.scn_range_.end_scn_.convert_for_tx(970));

  ObSSTable *incoming_sstable = nullptr;
  ObSSTable *old_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(incoming_key, incoming_sstable));
  ASSERT_EQ(OB_SUCCESS, mock_sstable(old_key, old_sstable, 300, 400));
  incoming_sstable->meta_cache_.min_merged_trans_version_ = logical_base;
  incoming_sstable->meta_cache_.max_merged_trans_version_ = logical_snapshot;
  incoming_sstable->meta_cache_.upper_trans_version_ = 1000;
  old_sstable->meta_cache_.min_merged_trans_version_ = logical_base;
  old_sstable->meta_cache_.max_merged_trans_version_ = logical_snapshot;
  old_sstable->meta_cache_.upper_trans_version_ = 1000;

  ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> old_tables;
  ASSERT_EQ(OB_SUCCESS, old_tables.push_back(old_sstable));
  ObTabletTableStore old_store;
  ASSERT_EQ(OB_SUCCESS, old_store.inc_major_tables_.init(allocator_, old_tables));

  ObTableHandleV2 incoming_handle;
  ASSERT_EQ(OB_SUCCESS, incoming_handle.set_sstable(incoming_sstable, &allocator_));
  ObBatchUpdateTableStoreParam param;
  ASSERT_EQ(OB_SUCCESS, param.tables_handle_.add_table(incoming_handle));
  ObTabletTableStore new_store;
  ASSERT_EQ(OB_SUCCESS, new_store.build_ha_inc_major_tables_(
      allocator_, true, param, old_store, 0));
  ASSERT_EQ(2, new_store.inc_major_tables_.count());
  EXPECT_TRUE((new_store.inc_major_tables_[0]->get_key() == incoming_key
               && new_store.inc_major_tables_[1]->get_key() == old_key)
      || (new_store.inc_major_tables_[0]->get_key() == old_key
          && new_store.inc_major_tables_[1]->get_key() == incoming_key));
}

TEST_F(TestTableStore, test_async_delete_major_uses_table_key_range)
{
  ObITable::TableKey major_key;
  major_key.tablet_id_ = tablet_id_;
  major_key.table_type_ = ObITable::MAJOR_SSTABLE;
  major_key.version_range_.base_version_ = 0;
  major_key.version_range_.snapshot_version_ = 150;
  ObSSTable *major = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(major_key, major));
  // A single transaction's rows do not describe the Major's refresh coverage.
  major->meta_cache_.min_merged_trans_version_ = 120;
  major->meta_cache_.max_merged_trans_version_ = 120;
  major->meta_->basic_meta_.min_merged_trans_version_ = 120;
  major->meta_->basic_meta_.max_merged_trans_version_ = 120;
  ObNewVersionRange range;
  ASSERT_EQ(OB_SUCCESS, major->get_async_index_version_range(range));
  EXPECT_EQ(ASYNC_INDEX_INITIAL_VERSION, range.base_version_);
  EXPECT_EQ(150, range.snapshot_version_);
  EXPECT_EQ(120, major->get_min_merged_trans_version());
  EXPECT_EQ(120, major->get_max_merged_trans_version());

  // 空 Baseline Major 的逻辑范围为 (0, 0]，snapshot version 允许为 0。
  major->key_.version_range_.snapshot_version_ =
      ASYNC_INDEX_INITIAL_VERSION;
  EXPECT_EQ(OB_SUCCESS, major->get_async_index_version_range(range));
  EXPECT_EQ(ASYNC_INDEX_INITIAL_VERSION, range.snapshot_version_);
  major->key_.version_range_.snapshot_version_ =
      ASYNC_INDEX_INITIAL_VERSION - 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, major->get_async_index_version_range(range));
  major->key_.version_range_.snapshot_version_ = 150;
  ASSERT_EQ(OB_SUCCESS, major->get_async_index_version_range(range));

  ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> major_tables;
  ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> inc_tables;
  ASSERT_EQ(OB_SUCCESS, major_tables.push_back(major));
  for (int64_t i = 0; i < 2; ++i) {
    ObITable::TableKey inc_key;
    inc_key.tablet_id_ = tablet_id_;
    inc_key.table_type_ = ObITable::INC_MAJOR_SSTABLE;
    ASSERT_EQ(OB_SUCCESS, inc_key.scn_range_.start_scn_.convert_for_tx(800 + i * 100));
    ASSERT_EQ(OB_SUCCESS, inc_key.scn_range_.end_scn_.convert_for_tx(850 + i * 100));
    ObSSTable *inc = nullptr;
    ASSERT_EQ(OB_SUCCESS, mock_sstable(inc_key, inc));
    inc->meta_cache_.min_merged_trans_version_ = 100 + i * 50;
    inc->meta_cache_.max_merged_trans_version_ = 150 + i * 50;
    inc->meta_->basic_meta_.min_merged_trans_version_ = 100 + i * 50;
    inc->meta_->basic_meta_.max_merged_trans_version_ = 150 + i * 50;
    ASSERT_EQ(OB_SUCCESS, inc_tables.push_back(inc));
  }
  ObTabletTableStore table_store;
  table_store.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, table_store.major_tables_.init(allocator_, major_tables));
  ASSERT_EQ(OB_SUCCESS, table_store.inc_major_tables_.init(allocator_, inc_tables));
  ObTablet tablet;
  tablet.table_store_cache_.is_async_index_refresh_target_ = true;
  tablet.table_store_cache_.is_async_delete_index_ = true;
  ObTableHandleV2 handle;
  ASSERT_EQ(1, table_store.major_tables_.count());
  ObSSTable *stored_major = table_store.major_tables_.at(0);
  ASSERT_NE(nullptr, stored_major);
  stored_major->key_.version_range_.snapshot_version_ =
      ASYNC_INDEX_INITIAL_VERSION;
  ASSERT_EQ(OB_SUCCESS, stored_major->get_async_index_version_range(range));
  ASSERT_EQ(OB_SUCCESS, table_store.get_async_index_sstable(
      ObStorageMetaHandle(), tablet, ObITable::MAJOR_SSTABLE, range, handle));
  ASSERT_NE(nullptr, handle.get_table());
  EXPECT_EQ(ASYNC_INDEX_INITIAL_VERSION,
      handle.get_table()->get_snapshot_version());
  handle.reset();
  stored_major->key_.version_range_.snapshot_version_ = 150;
  ASSERT_EQ(OB_SUCCESS, stored_major->get_async_index_version_range(range));
  ASSERT_EQ(OB_SUCCESS, table_store.get_async_index_sstable(
      ObStorageMetaHandle(), tablet, ObITable::MAJOR_SSTABLE, range, handle));
  ASSERT_NE(nullptr, handle.get_table());
  EXPECT_EQ(major_key, handle.get_table()->get_key());
  handle.reset();

  ObUpdateTableStoreParam param;
  param.sstable_ = major;
  param.compaction_info_.merge_type_ = ObMergeType::MAJOR_MERGE;
  ObStorageSchema storage_schema;
  storage_schema.index_type_ = INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL;
  param.storage_schema_ = &storage_schema;
  // Simulate ObTablet::init_for_merge(), where the new tablet cache has not
  // been initialized yet. The update schema is the authoritative type source.
  tablet.table_store_cache_.is_async_index_refresh_target_ = false;
  ASSERT_EQ(OB_SUCCESS, table_store.try_recycle_inc_major_tables_(allocator_, param));
  ASSERT_EQ(1, table_store.inc_major_tables_.count());
  EXPECT_EQ(150, table_store.inc_major_tables_[0]->get_async_index_base_version());
  EXPECT_EQ(200, table_store.inc_major_tables_[0]->get_async_index_snapshot_version());
}

TEST_F(TestTableStore, test_inc_major_medium_info_requires_positive_base)
{
  ServerModeGuard mode_guard(share::ObServerMode::SHARED_STORAGE_MODE);
  ObMediumCompactionInfo medium_info;
  medium_info.set_basic_info(
      ObMediumCompactionInfo::INC_MAJOR_COMPACTION,
      ObAdaptiveMergePolicy::NO_INC_DATA,
      2);
  medium_info.last_medium_snapshot_ = 2;
  medium_info.data_version_ = 1;
  medium_info.merge_base_version_ = 1;
  EXPECT_TRUE(medium_info.is_valid());

  medium_info.merge_base_version_ = 0;
  EXPECT_FALSE(medium_info.is_valid());

  medium_info.merge_base_version_ = medium_info.medium_snapshot_;
  EXPECT_FALSE(medium_info.is_valid());
}

TEST_F(TestTableStore, test_async_inc_major_sort_uses_logical_snapshot)
{
  ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> tables;
  for (int64_t i = 0; i < 2; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::INC_MAJOR_SSTABLE;
    ASSERT_EQ(OB_SUCCESS, key.scn_range_.start_scn_.convert_for_tx(800));
    ASSERT_EQ(OB_SUCCESS, key.scn_range_.end_scn_.convert_for_tx(980 - i * 10));
    ObSSTable *table = nullptr;
    ASSERT_EQ(OB_SUCCESS, mock_sstable(key, table));
    table->meta_cache_.min_merged_trans_version_ = 100 + i * 50;
    table->meta_cache_.max_merged_trans_version_ = 150 + i * 50;
    table->meta_cache_.upper_trans_version_ = 1000;
    table->meta_->basic_meta_.min_merged_trans_version_ = 100 + i * 50;
    table->meta_->basic_meta_.max_merged_trans_version_ = 150 + i * 50;
    table->meta_->basic_meta_.upper_trans_version_ = 1000;
    ASSERT_EQ(OB_SUCCESS, tables.push_back(table));
  }
  // Ordinary IncMajor keeps its existing physical snapshot ordering.
  ASSERT_EQ(OB_SUCCESS, ObTableStoreUtil::sort_inc_major_tables(false, tables));
  EXPECT_EQ(970, tables.at(0)->get_end_scn().get_val_for_tx());
  ObTabletTableStore old_store;
  ASSERT_EQ(OB_SUCCESS, old_store.inc_major_tables_.init(allocator_, tables));
  ObTablet tablet;
  tablet.table_store_cache_.is_async_index_refresh_target_ = true;
  tablet.table_store_cache_.is_async_delete_index_ = true;
  ObBatchUpdateTableStoreParam ha_param;
  ObTabletTableStore ha_store;
  ASSERT_EQ(OB_SUCCESS, ha_store.build_ha_inc_major_tables_(
      allocator_, tablet.is_async_index(), ha_param, old_store, 0));
  ASSERT_EQ(2, ha_store.inc_major_tables_.count());
  EXPECT_EQ(150, ha_store.inc_major_tables_[0]->get_async_index_snapshot_version());
  EXPECT_EQ(200, ha_store.inc_major_tables_[1]->get_async_index_snapshot_version());
  ha_store.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, ha_store.check_continuous(true));

  ha_param.need_replace_remote_sstable_ = true;
  ObTabletTableStore replace_store;
  ASSERT_EQ(OB_SUCCESS, replace_store.build_ha_inc_major_tables_(
      allocator_, tablet.is_async_index(), ha_param, old_store, 0));
  ASSERT_EQ(2, replace_store.inc_major_tables_.count());
  EXPECT_EQ(150, replace_store.inc_major_tables_[0]->get_async_index_snapshot_version());
  EXPECT_EQ(200, replace_store.inc_major_tables_[1]->get_async_index_snapshot_version());
  replace_store.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, replace_store.check_continuous(true));
#ifdef OB_BUILD_SHARED_STORAGE
  ObUpdateTableStoreParam param;
  ObStorageSchema storage_schema;
  storage_schema.index_type_ = INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL;
  param.storage_schema_ = &storage_schema;
  // ObTablet::init_for_merge() initializes the table store before the new
  // tablet cache. The update schema must therefore decide the sort mode.
  tablet.table_store_cache_.is_async_index_refresh_target_ = false;
  ObTabletTableStore ss_store;
  ASSERT_EQ(OB_SUCCESS, ss_store.inner_build_inc_major_tables_for_ss(
      allocator_, param, old_store, 0));
  ASSERT_EQ(2, ss_store.inc_major_tables_.count());
  EXPECT_EQ(150, ss_store.inc_major_tables_[0]->get_async_index_snapshot_version());
  EXPECT_EQ(200, ss_store.inc_major_tables_[1]->get_async_index_snapshot_version());
  ss_store.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, ss_store.check_ready_for_read_(tablet, true));
  EXPECT_TRUE(ss_store.is_ready_for_read_);
#endif
}

TEST_F(TestTableStore, test_deserialize_async_table_store_uses_schema_before_cache)
{
  ObTablet *tablet = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_tablet(tablet));
  ASSERT_FALSE(tablet->is_table_store_cache_inited());

  ObTableSchema table_schema;
  TestSchemaUtils::prepare_data_schema(table_schema);
  table_schema.set_table_type(USER_INDEX);
  table_schema.set_index_type(INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL);
  ASSERT_EQ(OB_SUCCESS,
      ObStorageSchemaUtil::alloc_storage_schema(
          allocator_, tablet->storage_schema_addr_.ptr_));
  ASSERT_EQ(OB_SUCCESS,
      tablet->storage_schema_addr_.get_ptr()->init(
          allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  ASSERT_TRUE(
      tablet->storage_schema_addr_.get_ptr()->is_async_index_refresh_target());

  ObSEArray<ObITable *, 2> inc_major_tables;
  for (int64_t i = 0; i < 2; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::INC_MAJOR_SSTABLE;
    ASSERT_EQ(OB_SUCCESS, key.scn_range_.start_scn_.convert_for_tx(800));
    ASSERT_EQ(OB_SUCCESS,
        key.scn_range_.end_scn_.convert_for_tx(980 - i * 10));
    ObSSTable *table = nullptr;
    ASSERT_EQ(OB_SUCCESS, mock_sstable(key, table));
    table->meta_->basic_meta_.min_merged_trans_version_ = 100 + i * 50;
    table->meta_->basic_meta_.max_merged_trans_version_ = 150 + i * 50;
    table->meta_->basic_meta_.upper_trans_version_ = 1000;
    table->meta_cache_.min_merged_trans_version_ = 100 + i * 50;
    table->meta_cache_.max_merged_trans_version_ = 150 + i * 50;
    table->meta_cache_.upper_trans_version_ = 1000;
    ASSERT_EQ(OB_SUCCESS, inc_major_tables.push_back(table));
  }

  ObTabletTableStore source_store;
  source_store.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS,
      source_store.inc_major_tables_.init(allocator_, inc_major_tables));
  ASSERT_EQ(OB_SUCCESS, source_store.check_continuous(true));
  ASSERT_EQ(OB_ERR_UNEXPECTED, source_store.check_continuous(false));

  const int64_t buf_len =
      source_store.get_serialize_size(DATA_VERSION_4_5_0_0);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t serialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      source_store.serialize(
          DATA_VERSION_4_5_0_0, buf, buf_len, serialize_pos));
  ASSERT_EQ(buf_len, serialize_pos);

  ObTabletTableStore deserialized_store;
  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      deserialized_store.deserialize(
          allocator_, *tablet, buf, buf_len, deserialize_pos));
  EXPECT_EQ(buf_len, deserialize_pos);
  EXPECT_TRUE(deserialized_store.is_ready_for_read_);
  EXPECT_EQ(OB_SUCCESS, deserialized_store.check_continuous(true));
  EXPECT_FALSE(tablet->is_table_store_cache_inited());
}

TEST_F(TestTableStore, test_async_inc_major_sort_validates_range)
{
  ObSSTable table;
  table.key_.table_type_ = ObITable::INC_MAJOR_SSTABLE;
  table.meta_cache_.min_merged_trans_version_ = 150;
  table.meta_cache_.max_merged_trans_version_ = 150;
  ObSEArray<ObITable *, OB_DEFAULT_SE_ARRAY_COUNT> tables;
  ASSERT_EQ(OB_SUCCESS, tables.push_back(&table));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObTableStoreUtil::sort_inc_major_tables(true, tables));
  table.meta_cache_.min_merged_trans_version_ =
      ASYNC_INDEX_MIN_INC_MAJOR_BASE_VERSION;
  table.meta_cache_.max_merged_trans_version_ =
      ASYNC_INDEX_MIN_INC_MAJOR_BASE_VERSION + 1;
  EXPECT_EQ(OB_SUCCESS, ObTableStoreUtil::sort_inc_major_tables(true, tables));
  int compare_ret = OB_SUCCESS;
  ObTableStoreUtil::ObAsyncIndexIncMajorCompare compare(compare_ret);
  EXPECT_FALSE(compare(nullptr, nullptr));
  EXPECT_EQ(OB_INVALID_ARGUMENT, compare_ret);
  compare_ret = OB_SUCCESS;
  EXPECT_FALSE(compare(nullptr, &table));
  EXPECT_EQ(OB_INVALID_ARGUMENT, compare_ret);
  compare_ret = OB_SUCCESS;
  EXPECT_TRUE(compare(&table, nullptr));
  EXPECT_EQ(OB_INVALID_ARGUMENT, compare_ret);
  table.meta_cache_.min_merged_trans_version_ =
      ASYNC_INDEX_MIN_INC_MAJOR_BASE_VERSION - 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObTableStoreUtil::sort_inc_major_tables(true, tables));
}

#ifdef OB_BUILD_SHARED_STORAGE
TEST_F(TestTableStore, test_sslog_inc_major_ref_scn)
{
  ObITable::TableKey table_key;
  table_key.tablet_id_ = tablet_id_;
  table_key.table_type_ = ObITable::TableType::INC_MAJOR_SSTABLE;
  ASSERT_EQ(OB_SUCCESS, table_key.scn_range_.start_scn_.convert_for_tx(0));
  ASSERT_EQ(OB_SUCCESS, table_key.scn_range_.end_scn_.convert_for_tx(80));
  ObSSTable *first_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(table_key, first_sstable));

  ASSERT_EQ(OB_SUCCESS, table_key.scn_range_.end_scn_.convert_for_tx(160));
  ObSSTable *second_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, mock_sstable(table_key, second_sstable, 300, 400));

  ObSEArray<ObITable *, 2> inc_major_tables;
  ASSERT_EQ(OB_SUCCESS, inc_major_tables.push_back(first_sstable));
  ASSERT_EQ(OB_SUCCESS, inc_major_tables.push_back(second_sstable));
  ObTabletTableStore table_store;
  table_store.is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, table_store.inc_major_tables_.init(allocator_, inc_major_tables));
  ObTablet tablet;
  tablet.table_store_addr_.ptr_ = &table_store;

  share::SCN raw_tablet_meta_scn;
  share::SCN reorg_scn;
  ASSERT_EQ(OB_SUCCESS, raw_tablet_meta_scn.convert_for_tx(30));
  ASSERT_EQ(OB_SUCCESS, reorg_scn.convert_for_tx(20));
  ASSERT_EQ(OB_SUCCESS, tablet.tablet_addr_.set_sslog_tablet_meta_addr(
      ObLSID(1), tablet_id_, reorg_scn, raw_tablet_meta_scn, 0, 4096));
  ASSERT_EQ(OB_SUCCESS, tablet.update_inc_major_ref_scn_(allocator_));
  ObSSTable *stored_first_sstable = table_store.inc_major_tables_.at(0);
  ObSSTable *stored_second_sstable = table_store.inc_major_tables_.at(1);
  ASSERT_NE(nullptr, stored_first_sstable);
  ASSERT_NE(nullptr, stored_second_sstable);
  ObSSTableMetaHandle first_meta_handle;
  ObSSTableMetaHandle second_meta_handle;
  ASSERT_EQ(OB_SUCCESS, stored_first_sstable->get_meta(first_meta_handle));
  ASSERT_EQ(OB_SUCCESS, stored_second_sstable->get_meta(second_meta_handle));
  EXPECT_EQ(raw_tablet_meta_scn,
      first_meta_handle.get_sstable_meta().get_basic_meta().get_shared_tablet_meta_scn());
  EXPECT_EQ(raw_tablet_meta_scn,
      second_meta_handle.get_sstable_meta().get_basic_meta().get_shared_tablet_meta_scn());
  ASSERT_EQ(OB_SUCCESS, tablet.init_aggregated_info(allocator_, nullptr));
  EXPECT_EQ(raw_tablet_meta_scn, tablet.tablet_meta_.min_inc_major_meta_scn_);

  ObTabletTableStore empty_table_store;
  empty_table_store.is_inited_ = true;
  ObTablet empty_tablet;
  empty_tablet.table_store_addr_.ptr_ = &empty_table_store;
  ASSERT_EQ(OB_SUCCESS, empty_tablet.init_aggregated_info(allocator_, nullptr));
  EXPECT_TRUE(empty_tablet.tablet_meta_.min_inc_major_meta_scn_.is_max());

  empty_tablet.table_store_addr_.ptr_ = nullptr;
  tablet.table_store_addr_.ptr_ = nullptr;
}
#endif


TEST_F(TestTableStore, test_multi_inc_major_table)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n"
              "27            0            110        110        120      \n"
              "27            0            130        130        140      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.inc_major_tables_.count());

  ObSSTable *new_inc_major = nullptr;
  ObITable::TableKey major_key;
  major_key.tablet_id_ = tablet_id_;
  major_key.scn_range_.start_scn_.set_min();
  major_key.scn_range_.end_scn_.convert_for_tx(150);
  major_key.table_type_ = ObITable::TableType::INC_MAJOR_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(major_key, new_inc_major));
  new_inc_major->meta_->basic_meta_.upper_trans_version_ = 170;
  new_inc_major->meta_cache_.upper_trans_version_ = 170;

  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 110;
  param.sstable_ = new_inc_major;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MINOR_MERGE;
  param.compaction_info_.need_report_ = false;

  // test add a new committed inc major
  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(3, new_table_store.inc_major_tables_.count());
}

TEST_F(TestTableStore, test_get_inc_major_read_tables)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n"
              "27            0            90         90         100      \n"
              "27            0            110        110        120      \n"
              "27            0            130        130        140      \n"
              "27            0            150        150        170      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(4, old_table_store.inc_major_tables_.count());

  const int64_t snapshot_version = 140;
  ObTableStoreIterator iter;
  old_table_store.is_ready_for_read_ = true;
  EXPECT_EQ(OB_SUCCESS, old_table_store.get_read_tables(snapshot_version, *tablet, iter));
  ObITable *table = nullptr;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(120, table->get_upper_trans_version());
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(140, table->get_upper_trans_version());
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(170, table->get_upper_trans_version());
  EXPECT_EQ(OB_ITER_END, iter.get_next(table));
}

TEST_F(TestTableStore, test_fork_source_inc_major_read_tables_with_filled_upper_version)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.tablet_id_ = tablet_id_;
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n"
              "27            0            90         90         100      \n"
              "27            0            110        110        120      \n"
              "27            0            130        130        140      \n"
              "27            0            150        150        170      \n";
  ObTabletTableStore table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&table_store));
  ASSERT_EQ(1, table_store.major_tables_.count());
  ASSERT_EQ(4, table_store.inc_major_tables_.count());

  const int64_t fork_snapshot_version = 140;
  ObForkTabletInfo fork_info;
  fork_info.set_src_tablet_id(tablet_id_);
  fork_info.set_fork_snapshot_version(fork_snapshot_version);

  ObTableStoreIterator iter;
  iter.set_fork_info(&fork_info);
  ASSERT_EQ(OB_SUCCESS,
      table_store.calculate_inc_major_read_tables_for_ss(
          fork_snapshot_version, *tablet, iter, table_store.major_tables_[0]));

  ObITable *table = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(120, table->get_upper_trans_version());
  ASSERT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(140, table->get_upper_trans_version());
  EXPECT_EQ(OB_ITER_END, iter.get_next(table));
}

TEST_F(TestTableStore, test_get_inc_major_read_tables_without_major)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "27            0            90         90         100      \n"
              "27            0            110        110        120      \n"
              "27            0            130        130        140      \n"
              "27            0            150        150        170      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(4, old_table_store.inc_major_tables_.count());

  const int64_t snapshot_version = 140;
  ObTableStoreIterator iter;
  old_table_store.is_ready_for_read_ = true;
  EXPECT_EQ(OB_SUCCESS, old_table_store.get_read_tables(snapshot_version, *tablet, iter, ObGetReadTablesMode::SKIP_MAJOR));
  ObITable *table = nullptr;
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(100, table->get_upper_trans_version());
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(120, table->get_upper_trans_version());
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(140, table->get_upper_trans_version());
  EXPECT_EQ(OB_SUCCESS, iter.get_next(table));
  EXPECT_EQ(ObITable::INC_MAJOR_SSTABLE, table->get_table_type());
  EXPECT_EQ(170, table->get_upper_trans_version());
  EXPECT_EQ(OB_ITER_END, iter.get_next(table));
}

TEST_F(TestTableStore, test_basic_add_new_inc_major_ddl_sstable)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(200);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            50         50         50       \n"
              "11            50           150        150        150      \n"
              "11            150          200        200        200      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.minor_tables_.count());


  // add a new inc major ddl sstable
  ObSSTable *new_inc_major_ddl_sstable = nullptr;
  ObITable::TableKey inc_major_ddl_key;
  inc_major_ddl_key.tablet_id_ = tablet_id_;
  inc_major_ddl_key.scn_range_.start_scn_.set_min();
  inc_major_ddl_key.scn_range_.end_scn_.convert_for_tx(250);
  inc_major_ddl_key.table_type_ = ObITable::TableType::INC_MAJOR_DDL_DUMP_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(inc_major_ddl_key, new_inc_major_ddl_sstable));
  new_inc_major_ddl_sstable->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  new_inc_major_ddl_sstable->meta_cache_.upper_trans_version_ = INT64_MAX;
  new_inc_major_ddl_sstable->meta_->basic_meta_.ddl_scn_ = tablet->get_tablet_meta().ddl_start_scn_;


  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 150;
  param.sstable_ = new_inc_major_ddl_sstable;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MINI_MERGE;
  param.compaction_info_.need_report_ = true;


  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(2, new_table_store.minor_tables_.count());
  EXPECT_EQ(0, new_table_store.inc_major_tables_.count());
  EXPECT_EQ(1, new_table_store.inc_major_ddl_sstables_.count());

  // add a new inc major sstable
  ObSSTable *new_inc_major_sstable = nullptr;
  ObITable::TableKey inc_major_key;
  inc_major_key.tablet_id_ = tablet_id_;
  inc_major_key.scn_range_.start_scn_.set_min();
  inc_major_key.scn_range_.end_scn_.convert_for_tx(250);
  inc_major_key.table_type_ = ObITable::TableType::INC_MAJOR_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(inc_major_key, new_inc_major_sstable));
  new_inc_major_sstable->meta_->basic_meta_.upper_trans_version_ = 250;
  new_inc_major_sstable->meta_cache_.upper_trans_version_ = 250;


  ObUpdateTableStoreParam param2;
  param2.multi_version_start_ = 150;
  param2.sstable_ = new_inc_major_sstable;
  param2.allow_duplicate_sstable_ = false;
  param2.compaction_info_.merge_type_ = ObMergeType::MAJOR_MERGE;
  param2.compaction_info_.need_report_ = true;


  ObTabletTableStore new_table_store2;
  EXPECT_EQ(OB_SUCCESS, new_table_store2.init(allocator_, *tablet, param2, new_table_store));
  EXPECT_EQ(1, new_table_store2.major_tables_.count());
  EXPECT_EQ(2, new_table_store2.minor_tables_.count());
  EXPECT_EQ(1, new_table_store2.inc_major_tables_.count());
  EXPECT_EQ(0, new_table_store2.inc_major_ddl_sstables_.count());

  // add a new inc major ddl sstable
  ObSSTable *new_inc_major_ddl_sstable2 = nullptr;
  ObITable::TableKey inc_major_ddl_key2;
  inc_major_ddl_key2.tablet_id_ = tablet_id_;
  inc_major_ddl_key2.scn_range_.start_scn_.set_min();
  inc_major_ddl_key2.scn_range_.end_scn_.convert_for_tx(250);
  inc_major_ddl_key2.table_type_ = ObITable::TableType::INC_MAJOR_DDL_DUMP_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(inc_major_ddl_key2, new_inc_major_ddl_sstable2));
  new_inc_major_ddl_sstable2->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  new_inc_major_ddl_sstable2->meta_cache_.upper_trans_version_ = INT64_MAX;
  new_inc_major_ddl_sstable2->meta_->basic_meta_.ddl_scn_ = tablet->get_tablet_meta().ddl_start_scn_;

  ObUpdateTableStoreParam param3;
  param3.multi_version_start_ = 150;
  param3.sstable_ = new_inc_major_ddl_sstable2;
  param3.allow_duplicate_sstable_ = false;
  param3.compaction_info_.merge_type_ = ObMergeType::MINI_MERGE;
  param3.compaction_info_.need_report_ = true;


  ObTabletTableStore new_table_store3;
  EXPECT_EQ(OB_SUCCESS, new_table_store3.init(allocator_, *tablet, param3, new_table_store2));
  EXPECT_EQ(1, new_table_store3.major_tables_.count());
  EXPECT_EQ(2, new_table_store3.minor_tables_.count());
  EXPECT_EQ(1, new_table_store3.inc_major_tables_.count());
  EXPECT_EQ(0, new_table_store3.inc_major_ddl_sstables_.count());
}

TEST_F(TestTableStore, test_compat_serialization)
{
  ObTablet *tablet = nullptr;
  EXPECT_EQ(OB_SUCCESS, mock_tablet(tablet));
  tablet->table_store_cache_.is_inited_ = true;
  tablet->tablet_meta_.clog_checkpoint_scn_.convert_for_tx(300);
  tablet->tablet_meta_.ha_status_.set_restore_status(ObTabletRestoreStatus::STATUS::FULL);
  tablet->tablet_meta_.ha_status_.set_data_status(ObTabletDataStatus::STATUS::COMPLETE);

  // add major and inc major 
  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n"
              "27            0            80         80         120      \n"
              "27            80           130        130        150      \n";
  ObTabletTableStore old_table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&old_table_store));
  EXPECT_EQ(1, old_table_store.major_tables_.count());
  EXPECT_EQ(2, old_table_store.inc_major_tables_.count());
  const int64_t boundary_version = ObTabletTableStore::TABLE_STORE_VERSION_V4;
  EXPECT_LT(boundary_version, old_table_store.version_);


  // add a new inc major ddl sstable
  ObSSTable *new_inc_major_ddl_sstable = nullptr;
  ObITable::TableKey inc_major_ddl_key;
  inc_major_ddl_key.tablet_id_ = tablet_id_;
  inc_major_ddl_key.scn_range_.start_scn_.convert_for_tx(130);
  inc_major_ddl_key.scn_range_.end_scn_.convert_for_tx(350);  // must >= clog_checkpoint_scn_(300) when MINI_MERGE
  inc_major_ddl_key.table_type_ = ObITable::TableType::INC_MAJOR_DDL_DUMP_SSTABLE;
  EXPECT_EQ(OB_SUCCESS, mock_sstable(inc_major_ddl_key, new_inc_major_ddl_sstable, 300, 400));
  new_inc_major_ddl_sstable->meta_->basic_meta_.upper_trans_version_ = INT64_MAX;
  new_inc_major_ddl_sstable->meta_cache_.upper_trans_version_ = INT64_MAX;
  new_inc_major_ddl_sstable->meta_->basic_meta_.ddl_scn_ = tablet->get_tablet_meta().ddl_start_scn_;

  ObUpdateTableStoreParam param;
  param.multi_version_start_ = 100;
  param.sstable_ = new_inc_major_ddl_sstable;
  param.allow_duplicate_sstable_ = false;
  param.compaction_info_.merge_type_ = ObMergeType::MINI_MERGE;
  param.compaction_info_.need_report_ = false;

  ObTabletTableStore new_table_store;
  EXPECT_EQ(OB_SUCCESS, new_table_store.init(allocator_, *tablet, param, old_table_store));
  EXPECT_EQ(1, new_table_store.major_tables_.count());
  EXPECT_EQ(2, new_table_store.inc_major_tables_.count());
  EXPECT_EQ(1, new_table_store.inc_major_ddl_sstables_.count());
  EXPECT_LT(boundary_version, new_table_store.version_);



  const int64_t old_data_version = DATA_VERSION_4_4_0_0;
  const int64_t cur_data_version = DATA_VERSION_4_5_0_0;

  const int64_t old_len = new_table_store.get_serialize_size(old_data_version);
  const int64_t cur_len = new_table_store.get_serialize_size(cur_data_version);
  EXPECT_LT(old_len, cur_len); // old_len < cur_len


  // serialize V5 table store with old data version
  char *old_buf = static_cast<char *>(allocator_.alloc(old_len));
  ASSERT_TRUE(nullptr != old_buf);
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, new_table_store.serialize(old_data_version, old_buf, old_len, pos));
  EXPECT_EQ(old_len, pos);

  // deserialize table store, the inc major tables should be empty
  ObTabletTableStore old_deserialized_table_store;
  int64_t pos2 = 0;
  EXPECT_EQ(OB_SUCCESS, old_deserialized_table_store.deserialize(allocator_, *tablet, old_buf, old_len, pos2));
  EXPECT_EQ(new_table_store.major_tables_.count(), old_deserialized_table_store.major_tables_.count());
  EXPECT_EQ(new_table_store.minor_tables_.count(), old_deserialized_table_store.minor_tables_.count());
  EXPECT_EQ(0, old_deserialized_table_store.inc_major_tables_.count());

  // serialize V5 table store with current data version
  char *cur_buf = static_cast<char *>(allocator_.alloc(cur_len));
  ASSERT_TRUE(nullptr != cur_buf);
  int64_t pos3 = 0;
  EXPECT_EQ(OB_SUCCESS, new_table_store.serialize(cur_data_version, cur_buf, cur_len, pos3));
  EXPECT_EQ(cur_len, pos3);

  // deserialize table store with current data version
  ObTabletTableStore deserialized_table_store;
  int64_t pos4 = 0;
  EXPECT_EQ(OB_SUCCESS, deserialized_table_store.deserialize(allocator_, *tablet, cur_buf, cur_len, pos4));
  EXPECT_EQ(new_table_store.major_tables_.count(), deserialized_table_store.major_tables_.count());
  EXPECT_EQ(new_table_store.minor_tables_.count(), deserialized_table_store.minor_tables_.count());
  EXPECT_EQ(new_table_store.inc_major_tables_.count(), deserialized_table_store.inc_major_tables_.count());
}


// Test major sstable recycle - not triggered when total count below threshold
TEST_F(TestTableStore, test_major_recycle_not_triggered_by_table_count)
{
  // total_table_count < MAJOR_RECYCLE_TRIGGER_THRESHOLD (134), should not recycle
  key_data_ = "table_type    start_scn    end_scn    max_ver    upper_ver\n"
              "10            0            100        100        100      \n"
              "10            0            200        200        200      \n"
              "10            0            300        300        300      \n"
              "11            300          400        400        400      \n";
  ObTabletTableStore table_store;
  EXPECT_EQ(OB_SUCCESS, mock_old_table_store(&table_store));
  EXPECT_EQ(3, table_store.major_tables_.count());
  EXPECT_EQ(1, table_store.minor_tables_.count());

  // total = 4, far below threshold (134), should not trigger recycle
  const int64_t original_major_count = table_store.major_tables_.count();
  EXPECT_EQ(OB_SUCCESS, table_store.try_recycle_major_tables_(allocator_));
  EXPECT_EQ(original_major_count, table_store.major_tables_.count());
}


// Test major sstable recycle - not triggered when major count <= min_keep_cnt
TEST_F(TestTableStore, test_major_recycle_not_triggered_by_major_count)
{
  // Even if we could exceed threshold, major_count <= MAJOR_RECYCLE_MIN_KEEP_CNT (8) should not recycle
  // This test verifies the major_count check logic
  ObTabletTableStore table_store;

  // Create only 5 majors (< min_keep_cnt = 8)
  ObArray<ObITable *> major_tables;
  for (int64_t i = 1; i <= 5; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    key.scn_range_.start_scn_.set_min();
    key.scn_range_.end_scn_.convert_for_tx(i * 100);
    ObSSTable *sstable = nullptr;
    EXPECT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.max_merged_trans_version_ = i * 100;
    sstable->meta_cache_.max_merged_trans_version_ = i * 100;
    EXPECT_EQ(OB_SUCCESS, major_tables.push_back(sstable));
  }

  // Create many minor tables to make total exceed threshold
  ObArray<ObITable *> minor_tables;
  for (int64_t i = 1; i <= 130; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MINOR_SSTABLE;
    key.scn_range_.start_scn_.convert_for_tx(500 + i);
    key.scn_range_.end_scn_.convert_for_tx(500 + i + 1);
    ObSSTable *sstable = nullptr;
    EXPECT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.upper_trans_version_ = 500 + i + 1;
    sstable->meta_cache_.upper_trans_version_ = 500 + i + 1;
    EXPECT_EQ(OB_SUCCESS, minor_tables.push_back(sstable));
  }

  EXPECT_EQ(OB_SUCCESS, table_store.major_tables_.init(allocator_, major_tables));
  EXPECT_EQ(OB_SUCCESS, table_store.minor_tables_.init(allocator_, minor_tables));
  table_store.is_inited_ = true;

  // total = 135 > threshold (134), but major_count = 5 <= min_keep_cnt (8)
  EXPECT_EQ(135, table_store.get_table_count());
  EXPECT_EQ(5, table_store.major_tables_.count());

  const int64_t original_major_count = table_store.major_tables_.count();
  EXPECT_EQ(OB_SUCCESS, table_store.try_recycle_major_tables_(allocator_));
  // Should NOT recycle because major_count <= min_keep_cnt
  EXPECT_EQ(original_major_count, table_store.major_tables_.count());
}


// Test major sstable recycle - basic recycle scenario
TEST_F(TestTableStore, test_major_recycle_basic)
{
  ObTabletTableStore table_store;

  // Create 50 major sstables
  ObArray<ObITable *> major_tables;
  for (int64_t i = 1; i <= 50; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    key.scn_range_.start_scn_.set_min();
    key.scn_range_.end_scn_.convert_for_tx(i * 100);
    ObSSTable *sstable = nullptr;
    EXPECT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.max_merged_trans_version_ = i * 100;
    sstable->meta_cache_.max_merged_trans_version_ = i * 100;
    EXPECT_EQ(OB_SUCCESS, major_tables.push_back(sstable));
  }

  // Create 90 minor tables to make total = 140 > threshold (134)
  ObArray<ObITable *> minor_tables;
  for (int64_t i = 1; i <= 90; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MINOR_SSTABLE;
    key.scn_range_.start_scn_.convert_for_tx(5000 + i);
    key.scn_range_.end_scn_.convert_for_tx(5000 + i + 1);
    ObSSTable *sstable = nullptr;
    EXPECT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.upper_trans_version_ = 5000 + i + 1;
    sstable->meta_cache_.upper_trans_version_ = 5000 + i + 1;
    EXPECT_EQ(OB_SUCCESS, minor_tables.push_back(sstable));
  }

  EXPECT_EQ(OB_SUCCESS, table_store.major_tables_.init(allocator_, major_tables));
  EXPECT_EQ(OB_SUCCESS, table_store.minor_tables_.init(allocator_, minor_tables));
  table_store.is_inited_ = true;

  EXPECT_EQ(140, table_store.get_table_count());
  EXPECT_EQ(50, table_store.major_tables_.count());

  EXPECT_EQ(OB_SUCCESS, table_store.try_recycle_major_tables_(allocator_));

  // After recycle:
  // - other_table_count = 90
  // - target_threshold = 57
  // - keep_count = MAX(57 - 90, 8) = 8 (capped by min_keep_cnt)
  // So should keep 8 majors
  EXPECT_EQ(8, table_store.major_tables_.count());

  // Verify oldest is kept (end_scn = 100)
  EXPECT_EQ(100, table_store.major_tables_[0]->get_key().scn_range_.end_scn_.get_val_for_tx());

  // Verify newest is kept (end_scn = 5000)
  const int64_t new_count = table_store.major_tables_.count();
  EXPECT_EQ(5000, table_store.major_tables_[new_count - 1]->get_key().scn_range_.end_scn_.get_val_for_tx());
}


// Test major sstable recycle - verify oldest and newest are always kept
TEST_F(TestTableStore, test_major_recycle_keeps_oldest_and_newest)
{
  ObTabletTableStore table_store;

  // Create 20 major sstables with specific versions
  ObArray<ObITable *> major_tables;
  for (int64_t i = 1; i <= 20; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    key.scn_range_.start_scn_.set_min();
    key.scn_range_.end_scn_.convert_for_tx(i * 1000);  // 1000, 2000, ..., 20000
    ObSSTable *sstable = nullptr;
    EXPECT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.max_merged_trans_version_ = i * 1000;
    sstable->meta_cache_.max_merged_trans_version_ = i * 1000;
    EXPECT_EQ(OB_SUCCESS, major_tables.push_back(sstable));
  }

  // Create 120 minor tables
  ObArray<ObITable *> minor_tables;
  for (int64_t i = 1; i <= 120; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MINOR_SSTABLE;
    key.scn_range_.start_scn_.convert_for_tx(20000 + i);
    key.scn_range_.end_scn_.convert_for_tx(20000 + i + 1);
    ObSSTable *sstable = nullptr;
    EXPECT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.upper_trans_version_ = 20000 + i + 1;
    sstable->meta_cache_.upper_trans_version_ = 20000 + i + 1;
    EXPECT_EQ(OB_SUCCESS, minor_tables.push_back(sstable));
  }

  EXPECT_EQ(OB_SUCCESS, table_store.major_tables_.init(allocator_, major_tables));
  EXPECT_EQ(OB_SUCCESS, table_store.minor_tables_.init(allocator_, minor_tables));
  table_store.is_inited_ = true;

  // total = 140 > threshold (134)
  EXPECT_EQ(140, table_store.get_table_count());

  const int64_t oldest_version = table_store.major_tables_[0]->get_key().scn_range_.end_scn_.get_val_for_tx();
  const int64_t newest_version = table_store.major_tables_[table_store.major_tables_.count() - 1]->get_key().scn_range_.end_scn_.get_val_for_tx();

  EXPECT_EQ(OB_SUCCESS, table_store.try_recycle_major_tables_(allocator_));

  // Verify oldest is still kept
  EXPECT_EQ(oldest_version, table_store.major_tables_[0]->get_key().scn_range_.end_scn_.get_val_for_tx());

  // Verify newest is still kept
  const int64_t new_count = table_store.major_tables_.count();
  EXPECT_EQ(newest_version, table_store.major_tables_[new_count - 1]->get_key().scn_range_.end_scn_.get_val_for_tx());
}


// Test major sstable recycle - verify newest K majors are kept consecutively
TEST_F(TestTableStore, test_major_recycle_keeps_newest_consecutive)
{
  ObTabletTableStore table_store;

  // Create 30 major sstables
  ObArray<ObITable *> major_tables;
  for (int64_t i = 1; i <= 30; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    key.scn_range_.start_scn_.set_min();
    key.scn_range_.end_scn_.convert_for_tx(i * 100);
    ObSSTable *sstable = nullptr;
    EXPECT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.max_merged_trans_version_ = i * 100;
    sstable->meta_cache_.max_merged_trans_version_ = i * 100;
    EXPECT_EQ(OB_SUCCESS, major_tables.push_back(sstable));
  }

  // Create 110 minor tables to make total = 140
  ObArray<ObITable *> minor_tables;
  for (int64_t i = 1; i <= 110; ++i) {
    ObITable::TableKey key;
    key.tablet_id_ = tablet_id_;
    key.table_type_ = ObITable::TableType::MINOR_SSTABLE;
    key.scn_range_.start_scn_.convert_for_tx(3000 + i);
    key.scn_range_.end_scn_.convert_for_tx(3000 + i + 1);
    ObSSTable *sstable = nullptr;
    EXPECT_EQ(OB_SUCCESS, mock_sstable(key, sstable));
    sstable->meta_->basic_meta_.upper_trans_version_ = 3000 + i + 1;
    sstable->meta_cache_.upper_trans_version_ = 3000 + i + 1;
    EXPECT_EQ(OB_SUCCESS, minor_tables.push_back(sstable));
  }

  EXPECT_EQ(OB_SUCCESS, table_store.major_tables_.init(allocator_, major_tables));
  EXPECT_EQ(OB_SUCCESS, table_store.minor_tables_.init(allocator_, minor_tables));
  table_store.is_inited_ = true;

  EXPECT_EQ(OB_SUCCESS, table_store.try_recycle_major_tables_(allocator_));

  const int64_t new_count = table_store.major_tables_.count();
  // keep_count = max(57 - 110, 8) = 8
  EXPECT_EQ(8, new_count);

  // MAJOR_RECYCLE_KEEP_NEWEST_CNT = 4, keep_count = 8, so keep_newest_cnt = min(4, max(8-1, 1)) = 4
  // So newest 4 should be consecutive (2700, 2800, 2900, 3000)

  // Check that the newest majors are consecutive
  const int64_t keep_newest_cnt = MIN(ObTabletTableStore::MAJOR_RECYCLE_KEEP_NEWEST_CNT, MAX(new_count - 1, 1));
  for (int64_t i = 0; i < keep_newest_cnt; ++i) {
    const int64_t expected_version = (30 - keep_newest_cnt + 1 + i) * 100;
    EXPECT_EQ(expected_version, table_store.major_tables_[new_count - keep_newest_cnt + i]->get_key().scn_range_.end_scn_.get_val_for_tx());
  }
}

} // unittest
} // oceanbase


int main(int argc, char **argv)
{
  system("rm -rf test_tablet_table_store.log*");
  OB_LOGGER.set_file_name("test_tablet_table_store.log");
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_tablet_table_store");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

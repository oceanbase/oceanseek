// owner: gaishun.gs
// owner group: storage

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#define protected public
#define private public

#include "mtlenv/storage/medium_info_helper.h"
#include "unittest/storage/test_tablet_helper.h"
#include "unittest/storage/test_dml_common.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include "storage/mds_table/ob_mds_tablet_schema.h"
#include "storage/mds_table/ob_mds_type_id_map.h"
#include "storage/mds_table/ob_mds_table_scan_iterator.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/tx_storage/ob_ls_service.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{
namespace {
bool ut_has_mds_memtable(const ObTablet *tablet)
{
  ObSEArray<ObITable *, 8> memtables;
  return OB_NOT_NULL(tablet)
      && OB_SUCCESS == tablet->inner_get_memtables(memtables)
      && memtables.count() > 0;
}

// Mirrors former ObITabletMdsInterface::get_ls_mds_tablet_handle_for_read (test-only).
int get_ls_mds_tablet_handle_for_read_for_test(const ObITabletMdsInterface &tablet_mds,
                                               ObTabletHandle &out_handle)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_mds.get_tablet_meta_().ls_id_;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("fail to get ls for ls mds tablet read", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id));
  } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet(LS_MDS_TABLET, out_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("fail to get ls mds inner tablet", K(ret), K(ls_id));
    }
  }
  return ret;
}

int init_mds_scan_iterator_on_ls_mds_tablet(
    const ObITabletMdsInterface &tablet_mds,
    ObTableScanParam &scan_param,
    ObStoreCtx &store_ctx,
    ObMdsTableScanIterator &iter)
{
  int ret = OB_SUCCESS;
  ObTabletHandle ls_mds_handle;
  ObMdsTabletSchemaHelper &tablet_sch = ObMdsTabletSchemaHelper::get_instance();
  share::SCN read_snapshot;
  if (OB_UNLIKELY(!tablet_sch.is_inited())) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(read_snapshot.convert_for_tx(scan_param.read_version_range_.snapshot_version_))) {
    LOG_WARN("failed to convert read snapshot for mds scan", K(ret), K(scan_param.read_version_range_));
  } else if (!store_ctx.is_valid()
             && OB_FAIL(store_ctx.init_for_read(
                    scan_param.ls_id_, LS_MDS_TABLET, scan_param.timeout_, -1, read_snapshot))) {
    LOG_WARN("failed to init store ctx for mds scan", K(ret), K(scan_param.ls_id_), K(read_snapshot));
  } else if (OB_FAIL(get_ls_mds_tablet_handle_for_read_for_test(tablet_mds, ls_mds_handle))) {
    LOG_WARN("failed to get ls mds tablet handle for read", K(ret));
  } else if (OB_FAIL(iter.init(scan_param, ls_mds_handle, store_ctx))) {
    LOG_WARN("failed to init mds table scan iterator", K(ret));
  }
  return ret;
}

template <typename Key, typename Value>
int ut_scan_mds_key_at_snapshot_version(
    common::ObIAllocator &allocator,
    const ObITabletMdsInterface &tablet_mds,
    const share::ObLSID &ls_id,
    const common::ObTabletID &user_tablet_id,
    const share::SCN &transfer_scn,
    const Key &key,
    const int64_t snapshot_version,
    const int64_t timeout_us,
    Value &value)
{
  int ret = OB_SUCCESS;
  char key_buf[ObMdsTabletSchemaDef::MDS_KEY_COLUMN_LENGTH];
  int64_t pos = 0;
  if (OB_FAIL(key.mds_serialize(key_buf, static_cast<int64_t>(sizeof(key_buf)), pos))) {
    STORAGE_LOG(WARN, "failed to serialize mds key for ut scan", K(ret));
  } else {
    const common::ObString udf_key(static_cast<int32_t>(pos), key_buf);
    constexpr uint8_t mds_unit_id = static_cast<uint8_t>(mds_type_id_detail::TypeIdOf<Key, Value>::value);
    ObTableScanParam scan_param;
    ObStoreCtx store_ctx;
    ObMdsTableScanIterator iter;
    Key read_key;
    ObMdsValueMeta mds_meta;
    if (OB_FAIL(ObMdsScanParamHelper::build_mds_unit_range_scan_param(
            allocator, ls_id, user_tablet_id, transfer_scn, mds_unit_id,
            timeout_us, ObVersionRange(0, snapshot_version),
            false/*is_select_follower*/, scan_param))) {
      STORAGE_LOG(WARN, "failed to build mds scan param for ut", K(ret), K(user_tablet_id), K(key));
    } else if (OB_FAIL(init_mds_scan_iterator_on_ls_mds_tablet(tablet_mds, scan_param, store_ctx, iter))) {
      STORAGE_LOG(WARN, "failed to init mds scan iterator for ut", K(ret), K(user_tablet_id), K(key));
    } else {
      while (OB_SUCC(ret)) {
        read_key.reset();
        value.reset();
        mds_meta.reset();
        if (OB_FAIL(iter.get_next_mds_kv(allocator, read_key, value, mds_meta))) {
          if (OB_ITER_END == ret) {
            ret = OB_EMPTY_RESULT;
          }
        } else {
          char read_key_buf[ObMdsTabletSchemaDef::MDS_KEY_COLUMN_LENGTH];
          int64_t read_pos = 0;
          if (OB_FAIL(read_key.mds_serialize(read_key_buf, static_cast<int64_t>(sizeof(read_key_buf)), read_pos))) {
            STORAGE_LOG(WARN, "failed to serialize read key", K(ret));
          } else if (read_pos == pos
                     && 0 == MEMCMP(key_buf, read_key_buf, pos)) {
            break;
          }
        }
      }
    }
  }
  return ret;
}
} // namespace

class TestMdsTableScan : public::testing::Test
{
public:
  TestMdsTableScan() = default;
  virtual ~TestMdsTableScan() = default;
public:
  static void SetUpTestCase();
  static void TearDownTestCase();
public:
  static int create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle);
  static int remove_ls(const share::ObLSID &ls_id);
  int create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
  static int get_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle);
  static int get_ls_mds_tablet(ObTabletHandle &tablet_handle);
  static int wait_for_mds_table_flush(const common::ObTabletID &tablet_id);
  static int wait_for_all_mds_nodes_released(const common::ObTabletID &tablet_id);
  static int try_schedule_mds_minor(const common::ObTabletID &tablet_id);
public:
  static constexpr uint64_t TENANT_ID = 1001;
  static const share::ObLSID LS_ID;
public:
  common::ObArenaAllocator allocator_;
};

const share::ObLSID TestMdsTableScan::LS_ID(1234);

void TestMdsTableScan::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObServerStorageMetaService::get_instance().is_started_ = true;

  // create ls
  ObLSHandle ls_handle;
  ret = create_ls(TENANT_ID, LS_ID, ls_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestMdsTableScan::TearDownTestCase()
{
  int ret = OB_SUCCESS;

  // remove ls
  ret = remove_ls(LS_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

int TestMdsTableScan::create_ls(const uint64_t tenant_id, const share::ObLSID &ls_id, ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ret = TestDmlCommon::create_ls(tenant_id, ls_id, ls_handle);
  return ret;
}

int TestMdsTableScan::remove_ls(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ls_id);
  return ret;
}

int TestMdsTableScan::create_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = 1234567;
  share::schema::ObTableSchema table_schema;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), KP(ls));
  } else if (OB_FAIL(build_test_schema(table_schema, table_id))) {
    LOG_WARN("failed to build table schema");
  } else if (OB_FAIL(TestTabletHelper::create_tablet(ls_handle, tablet_id, table_schema, allocator_,
      ObTabletStatus::MAX, share::SCN::invalid_scn(), tablet_handle))) {
    LOG_WARN("failed to create tablet", K(ret));
  }

  return ret;
}

int TestMdsTableScan::get_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_tablet_svr()->direct_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), KP(ls));
  }

  return ret;
}

int TestMdsTableScan::get_ls_mds_tablet(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), KP(ls));
  } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet(LS_MDS_TABLET, tablet_handle))) {
    LOG_WARN("failed to get ls mds tablet", K(ret));
  }
  return ret;
}

int TestMdsTableScan::wait_for_mds_table_flush(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  UNUSED(tablet_id);

  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  int times = 0;
  ret = TestMdsTableScan::get_ls_mds_tablet(tablet_handle);
  EXPECT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  EXPECT_NE(nullptr, tablet);
  do {
    ret = TestMdsTableScan::get_ls_mds_tablet(tablet_handle);
    EXPECT_EQ(OB_SUCCESS, ret);
    tablet = tablet_handle.get_obj();
    EXPECT_NE(nullptr, tablet);
    if (ut_has_mds_memtable(tablet)) {
      break;
    }
    ::ob_usleep(100_ms);
    ++times;
  } while (times < 50);

  // LS aggregate MDS: after flush_mds_table, data stays on LS_MDS_TABLET memtable.
  ret = TestMdsTableScan::get_ls_mds_tablet(tablet_handle);
  EXPECT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  EXPECT_NE(nullptr, tablet);
  EXPECT_TRUE(ut_has_mds_memtable(tablet));

  if (::testing::Test::HasFailure()) {
    ret = OB_TIMEOUT;
  }

  return ret;
}

int TestMdsTableScan::wait_for_all_mds_nodes_released(const common::ObTabletID &tablet_id)
{
  UNUSED(tablet_id);
  // Per-tablet MdsTableHandle node counts are gone; LS aggregate release follows flush/minor timing.
  for (int times = 0; times < 60; ++times) {
    ::ob_usleep(100_ms);
  }
  return OB_SUCCESS;
}

int TestMdsTableScan::try_schedule_mds_minor(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObApplyStatusGuard guard;
  if (GCONF.enable_logservice) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("logservice is not supported", K(ret));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls", K(ret));
  } else {
    int times = 0;
    do
    {
      STORAGE_LOG(INFO, "try mds minor", K(times));
      if (OB_FAIL(MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD))) {
        LOG_WARN("failed to get ls", K(ret), K(LS_ID));
      } else if (OB_FAIL(TestMdsTableScan::get_ls_mds_tablet(tablet_handle))) {
        LOG_WARN("failed to get ls mds tablet", K(ret));
      // for pass schedule varify
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is null", K(ret), KP(ls));
      } else if (OB_FAIL(ls->log_handler_.apply_service_->get_apply_status(LS_ID, guard))) {
        LOG_WARN("failed to get apply status", K(ret), K(LS_ID));
      } else if (OB_FALSE_IT(guard.apply_status_->max_applied_cb_scn_ = share::SCN::plus(share::SCN::min_scn(), 350+100))) {
      } else if (OB_FALSE_IT(guard.apply_status_->last_check_scn_ = share::SCN::plus(share::SCN::min_scn(), 350+100))) {
      } else if (OB_FALSE_IT(static_cast<palf::PalfHandleImpl* >(static_cast<palf::PalfHandle*>(guard.apply_status_->palf_handle_)->palf_handle_impl_)->sw_.lsn_allocator_.lsn_ts_meta_.scn_delta_ =450)) {
      } else if (OB_FAIL(compaction::ObTenantTabletScheduler::schedule_tablet_minor_merge<compaction::ObTabletMergeExecuteDag>(
          compaction::MINOR_MERGE, ls_handle, tablet_handle))) {
        STORAGE_LOG(WARN, "fail to schedule minor merge on ls mds tablet", K(ret));
      }
      // sleep
      ::ob_usleep(100_ms);
      ++times;
    } while (OB_EAGAIN == ret && times < 20);

    if (OB_SUCC(ret)) {
      times = 0;
      const ObTabletTableStore *table_store = nullptr;
      do {
        STORAGE_LOG(INFO, "waiting for mds minor finish", K(times));
        ret = TestMdsTableScan::get_ls_mds_tablet(tablet_handle);
        EXPECT_EQ(OB_SUCCESS, ret);
        ObTablet *tablet = tablet_handle.get_obj();
        // check mds sstable on LS aggregate MDS tablet
        ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
        ret = tablet->fetch_table_store(table_store_wrapper);
        EXPECT_EQ(OB_SUCCESS, ret);
        table_store = table_store_wrapper.get_member();
        if (!ut_has_mds_memtable(tablet)
            && table_store->get_minor_sstables().count() < 1) {
          ret = OB_EAGAIN;
          STORAGE_LOG(WARN, "fail to finish mds minor", KPC(table_store));
        }
        ::ob_usleep(100_ms);
        ++times;
      } while (OB_EAGAIN == ret && times < 20);

      if (OB_SUCC(ret)) {
        STORAGE_LOG(INFO, "success finish mds minor", KPC(table_store));
      }
    } else {
      STORAGE_LOG(WARN, "fail to schedule minor merge", K(ret));
    }
  }

  return ret;
}
TEST_F(TestMdsTableScan, tablet_status)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 100);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  share::SCN mds_checkpoint_scn = tablet->get_clog_checkpoint_scn();
  ASSERT_EQ(share::SCN::base_scn(), mds_checkpoint_scn);

  // write data to mds table
  {
    ObTabletCreateDeleteMdsUserData user_data;
    ObLSHandle ls_handle;
    share::SCN replay_scn;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, build_test_create_tablet_mds_user_data(
        ObTabletStatus::NORMAL, create_commit_scn, share::SCN::invalid_scn(), user_data));
    ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
        *ls_handle.get_ls(), create_commit_scn, replay_scn));
    ret = ut_replay_tablet_status_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, replay_scn, transaction::ObTransID(123));
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  // flush LS aggregate MDS (LS_MDS_TABLET)
  {
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = ls_handle.get_ls()->flush_mds_table();
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read mds sstable via tablet interface
  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();

  const int64_t timeout_us = ObClockGenerator::getClock() + 1_s;
  ObTabletCreateDeleteMdsUserData read_data;
  SCN snapshot = share::SCN::plus(share::SCN::min_scn(), 120);
  ret = tablet->get_snapshot<ObTabletCreateDeleteMdsUserData>(
      read_data, snapshot, timeout_us);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletStatus::NORMAL, read_data.tablet_status_);
  ASSERT_EQ(create_commit_scn, read_data.create_commit_scn_);
  ASSERT_EQ(100, read_data.create_commit_version_);

  snapshot = share::SCN::plus(share::SCN::min_scn(), 10);
  ret = tablet->get_snapshot<ObTabletCreateDeleteMdsUserData>(
      [](const ObTabletCreateDeleteMdsUserData &) { return OB_SUCCESS; },
      snapshot,
      timeout_us);
  ASSERT_EQ(OB_EMPTY_RESULT, ret);
}

TEST_F(TestMdsTableScan, aux_tablet_info)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 100);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  share::SCN mds_checkpoint_scn = tablet->get_clog_checkpoint_scn();
  ASSERT_EQ(share::SCN::base_scn(), mds_checkpoint_scn);

  // write data to mds table via replay (LS aggregate path)
  {
    ObLSHandle ls_handle;
    ObTabletCreateDeleteMdsUserData user_data;
    share::SCN replay_scn;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, build_test_create_tablet_mds_user_data(
        ObTabletStatus::NORMAL, create_commit_scn, share::SCN::invalid_scn(), user_data));
    ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
        *ls_handle.get_ls(), create_commit_scn, replay_scn));
    ASSERT_EQ(OB_SUCCESS, ut_replay_tablet_status_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, replay_scn, transaction::ObTransID(123)));
  }

  {
    ObLSHandle ls_handle;
    ObTabletBindingMdsUserData user_data;
    user_data.data_tablet_id_ = 100;
    user_data.hidden_tablet_id_ = 101;
    user_data.snapshot_version_ = 9527;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 120);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, ut_replay_ddl_info_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, commit_scn, transaction::ObTransID(456)));
  }

  // mds table flush
  {
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = ls_handle.get_ls()->flush_mds_table();
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read mds sstable
  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();

  const int64_t timeout_us = ObClockGenerator::getClock() + 1_s;
  ObTabletBindingMdsUserData read_data;
  SCN snapshot = share::SCN::plus(share::SCN::min_scn(), 130);
  ret = tablet->get_snapshot<ObTabletBindingMdsUserData>(
      read_data, snapshot, timeout_us);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(100, read_data.data_tablet_id_.id_);
  ASSERT_EQ(101, read_data.hidden_tablet_id_.id_);
  ASSERT_EQ(9527, read_data.snapshot_version_);
}

TEST_F(TestMdsTableScan, medium_info)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 100);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  share::SCN mds_checkpoint_scn = tablet->get_clog_checkpoint_scn();
  ASSERT_EQ(share::SCN::base_scn(), mds_checkpoint_scn);

  // write data to mds table via replay
  {
    ObLSHandle ls_handle;
    ObTabletCreateDeleteMdsUserData user_data;
    share::SCN replay_scn;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, build_test_create_tablet_mds_user_data(
        ObTabletStatus::NORMAL, create_commit_scn, share::SCN::invalid_scn(), user_data));
    ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
        *ls_handle.get_ls(), create_commit_scn, replay_scn));
    ASSERT_EQ(OB_SUCCESS, ut_replay_tablet_status_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, replay_scn, transaction::ObTransID(123)));
  }

  {
    ObLSHandle ls_handle;
    compaction::ObMediumCompactionInfoKey key(100);
    compaction::ObMediumCompactionInfo info;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 120);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info, 100);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, ut_replay_keyed_mds_for_test(
        *ls_handle.get_ls(), tablet_id, key, info, commit_scn, transaction::ObTransID(777)));
  }

  {
    ObLSHandle ls_handle;
    compaction::ObMediumCompactionInfoKey key(200);
    compaction::ObMediumCompactionInfo info;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 220);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info, 200);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, ut_replay_keyed_mds_for_test(
        *ls_handle.get_ls(), tablet_id, key, info, commit_scn, transaction::ObTransID(888)));
  }

  // mds table flush
  {
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = ls_handle.get_ls()->flush_mds_table();
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // read mds sstable
  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();

  compaction::ObMediumCompactionInfo medium_info;
  const share::SCN read_snapshot = share::SCN::max_scn();
  ret = tablet->get_snapshot<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      compaction::ObMediumCompactionInfoKey(100),
      medium_info,
      read_snapshot,
      1_s/*timeout_us*/,
      &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(100, medium_info.medium_snapshot_);

  medium_info.reset();
  ret = tablet->get_snapshot<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      compaction::ObMediumCompactionInfoKey(105),
      medium_info,
      read_snapshot,
      1_s/*timeout_us*/,
      &allocator_);
  ASSERT_EQ(OB_EMPTY_RESULT, ret);

  medium_info.reset();
  ret = tablet->get_snapshot<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      compaction::ObMediumCompactionInfoKey(200),
      medium_info,
      read_snapshot,
      1_s/*timeout_us*/,
      &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(200, medium_info.medium_snapshot_);

  medium_info.reset();
  ret = ut_scan_mds_key_at_snapshot_version(
      allocator_, *tablet, LS_ID, tablet_id, tablet->get_reorganization_scn(),
      compaction::ObMediumCompactionInfoKey(200), 150, 1_s, medium_info);
  ASSERT_EQ(OB_EMPTY_RESULT, ret);

  medium_info.reset();
  ret = ut_scan_mds_key_at_snapshot_version(
      allocator_, *tablet, LS_ID, tablet_id, tablet->get_reorganization_scn(),
      compaction::ObMediumCompactionInfoKey(100), 150, 1_s, medium_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(100, medium_info.medium_snapshot_);
}

TEST_F(TestMdsTableScan, multi_version_row)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 100);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  share::SCN mds_checkpoint_scn = tablet->get_clog_checkpoint_scn();
  ASSERT_EQ(share::SCN::base_scn(), mds_checkpoint_scn);

  // write data to mds table no.1 row via replay
  {
    ObLSHandle ls_handle;
    ObTabletCreateDeleteMdsUserData user_data;
    share::SCN replay_scn;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, build_test_create_tablet_mds_user_data(
        ObTabletStatus::NORMAL, create_commit_scn, share::SCN::invalid_scn(), user_data));
    ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
        *ls_handle.get_ls(), create_commit_scn, replay_scn));
    ASSERT_EQ(OB_SUCCESS, ut_replay_tablet_status_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, replay_scn, transaction::ObTransID(123)));
  }

  // write data to mds table no.2 row via replay
  {
    ObLSHandle ls_handle;
    ObTabletBindingMdsUserData user_data;
    user_data.data_tablet_id_ = 100;
    user_data.hidden_tablet_id_ = 101;
    user_data.snapshot_version_ = 9527;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 200);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, ut_replay_ddl_info_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, commit_scn, transaction::ObTransID(256)));
  }

  share::SCN transfer_out_commit_scn = share::SCN::plus(share::SCN::min_scn(), 300);
  // write data to mds table no.3 row via replay
  {
    ObLSHandle ls_handle;
    ObTabletCreateDeleteMdsUserData user_data;
    share::SCN replay_scn;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    build_test_transfer_out_tablet_status(transfer_out_commit_scn, user_data);
    ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
        *ls_handle.get_ls(), transfer_out_commit_scn, replay_scn));
    ASSERT_EQ(OB_SUCCESS, ut_replay_tablet_status_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, replay_scn, transaction::ObTransID(356)));
  }

  // mds table flush
  {
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = ls_handle.get_ls()->flush_mds_table();
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait all mds nodes to be released
  tablet_handle.reset();
  ret = wait_for_all_mds_nodes_released(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  const int64_t timeout_us = ObClockGenerator::getClock() + 1_s;

  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // get tablet status with 330
  ObTabletCreateDeleteMdsUserData create_del_data;
  SCN snapshot = share::SCN::plus(share::SCN::min_scn(), 330);
  ret = tablet->get_snapshot<ObTabletCreateDeleteMdsUserData>(
      create_del_data, snapshot, timeout_us);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletStatus::TRANSFER_OUT, create_del_data.tablet_status_);
  ASSERT_EQ(ObTabletMdsUserDataType::START_TRANSFER_OUT, create_del_data.data_type_);
  ASSERT_EQ(transfer_out_commit_scn, create_del_data.transfer_scn_);
  ASSERT_EQ(300, create_del_data.start_transfer_commit_version_);

  // get tablet status with 110
  create_del_data.reset();
  snapshot = share::SCN::plus(share::SCN::min_scn(), 110);
  ret = tablet->get_snapshot<ObTabletCreateDeleteMdsUserData>(
      create_del_data, snapshot, timeout_us);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletStatus::NORMAL, create_del_data.tablet_status_);
  ASSERT_EQ(ObTabletMdsUserDataType::CREATE_TABLET, create_del_data.data_type_);
  ASSERT_EQ(create_commit_scn, create_del_data.create_commit_scn_);
  ASSERT_EQ(100, create_del_data.create_commit_version_);

  // get aux_tablet_info with 220
  ObTabletBindingMdsUserData binding_data;
  snapshot = share::SCN::plus(share::SCN::min_scn(), 220);
  ret = tablet->get_snapshot<ObTabletBindingMdsUserData>(
      binding_data, snapshot, timeout_us);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(100, binding_data.data_tablet_id_.id_);
  ASSERT_EQ(101, binding_data.hidden_tablet_id_.id_);
  ASSERT_EQ(9527, binding_data.snapshot_version_);

  // get aux_tablet_info with 190
  binding_data.reset();
  snapshot = share::SCN::plus(share::SCN::min_scn(), 190);
  ret = tablet->get_snapshot<ObTabletBindingMdsUserData>(
      binding_data, snapshot, timeout_us);
  ASSERT_EQ(OB_EMPTY_RESULT, ret);
}

TEST_F(TestMdsTableScan, test_minor_scan)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 100);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  share::SCN mds_checkpoint_scn = tablet->get_clog_checkpoint_scn();
  ASSERT_EQ(share::SCN::base_scn(), mds_checkpoint_scn);

  // write data to mds table no.1 row via replay
  {
    ObLSHandle ls_handle;
    ObTabletCreateDeleteMdsUserData user_data;
    share::SCN replay_scn;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, build_test_create_tablet_mds_user_data(
        ObTabletStatus::NORMAL, create_commit_scn, share::SCN::invalid_scn(), user_data));
    ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
        *ls_handle.get_ls(), create_commit_scn, replay_scn));
    ASSERT_EQ(OB_SUCCESS, ut_replay_tablet_status_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, replay_scn, transaction::ObTransID(123)));
  }

  // write data to mds table no.2 row via replay
  {
    ObLSHandle ls_handle;
    ObTabletBindingMdsUserData user_data;
    user_data.data_tablet_id_ = 100;
    user_data.hidden_tablet_id_ = 101;
    user_data.snapshot_version_ = 9527;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 200);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, ut_replay_ddl_info_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, commit_scn, transaction::ObTransID(256)));
  }

  // flush LS aggregate MDS once
  {
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = ls_handle.get_ls()->flush_mds_table();
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait all mds nodes to be released
  tablet_handle.reset();
  ret = wait_for_all_mds_nodes_released(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  // write data to mds table no.3 row via replay
  {
    ObLSHandle ls_handle;
    compaction::ObMediumCompactionInfoKey key(250);
    compaction::ObMediumCompactionInfo info;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 270);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info, 250);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, ut_replay_keyed_mds_for_test(
        *ls_handle.get_ls(), tablet_id, key, info, commit_scn, transaction::ObTransID(300)));
  }
  // write data to mds table no.4 row via replay
  {
    ObLSHandle ls_handle;
    compaction::ObMediumCompactionInfoKey key(270);
    compaction::ObMediumCompactionInfo info;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 290);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info, 270);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, ut_replay_keyed_mds_for_test(
        *ls_handle.get_ls(), tablet_id, key, info, commit_scn, transaction::ObTransID(320)));
  }

  share::SCN transfer_out_commit_scn = share::SCN::plus(share::SCN::min_scn(), 300);
  // write data to mds table no.5 row via replay
  {
    ObLSHandle ls_handle;
    ObTabletCreateDeleteMdsUserData user_data;
    share::SCN replay_scn;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    build_test_transfer_out_tablet_status(transfer_out_commit_scn, user_data);
    ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
        *ls_handle.get_ls(), transfer_out_commit_scn, replay_scn));
    ASSERT_EQ(OB_SUCCESS, ut_replay_tablet_status_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, replay_scn, transaction::ObTransID(356)));
  }

  // flush LS aggregate MDS again (hope to build minor sstable)
  {
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = ls_handle.get_ls()->flush_mds_table();
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush except->triggle minor
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait all mds nodes to be released
  tablet_handle.reset();
  ret = wait_for_all_mds_nodes_released(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // schedule mds minor
  ASSERT_EQ(OB_SUCCESS, try_schedule_mds_minor(tablet_id));
  // test multi version get
  const int64_t timeout_us = ObClockGenerator::getClock() + 1_s;

  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // get tablet status with 330
  ObTabletCreateDeleteMdsUserData create_del_data;
  SCN snapshot = share::SCN::plus(share::SCN::min_scn(), 330);
  ret = tablet->get_snapshot<ObTabletCreateDeleteMdsUserData>(
      create_del_data, snapshot, timeout_us);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletStatus::TRANSFER_OUT, create_del_data.tablet_status_);
  ASSERT_EQ(ObTabletMdsUserDataType::START_TRANSFER_OUT, create_del_data.data_type_);
  ASSERT_EQ(transfer_out_commit_scn, create_del_data.transfer_scn_);
  ASSERT_EQ(300, create_del_data.start_transfer_commit_version_);

  // get tablet status with 110
  create_del_data.reset();
  snapshot = share::SCN::plus(share::SCN::min_scn(), 110);
  ret = tablet->get_snapshot<ObTabletCreateDeleteMdsUserData>(
      create_del_data, snapshot, timeout_us);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletStatus::NORMAL, create_del_data.tablet_status_);
  ASSERT_EQ(ObTabletMdsUserDataType::CREATE_TABLET, create_del_data.data_type_);
  ASSERT_EQ(create_commit_scn, create_del_data.create_commit_scn_);
  ASSERT_EQ(100, create_del_data.create_commit_version_);

  // get aux_tablet_info with 220
  ObTabletBindingMdsUserData binding_data;
  snapshot = share::SCN::plus(share::SCN::min_scn(), 220);
  ret = tablet->get_snapshot<ObTabletBindingMdsUserData>(
      binding_data, snapshot, timeout_us);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(100, binding_data.data_tablet_id_.id_);
  ASSERT_EQ(101, binding_data.hidden_tablet_id_.id_);
  ASSERT_EQ(9527, binding_data.snapshot_version_);

  // get aux_tablet_info with 190
  binding_data.reset();
  snapshot = share::SCN::plus(share::SCN::min_scn(), 190);
  ret = tablet->get_snapshot<ObTabletBindingMdsUserData>(
      binding_data, snapshot, timeout_us);
  ASSERT_EQ(OB_EMPTY_RESULT, ret);

  // get medium_compaction_info
  compaction::ObMediumCompactionInfo medium_info;
  const share::SCN read_snapshot = share::SCN::max_scn();
  ret = tablet->get_snapshot<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      compaction::ObMediumCompactionInfoKey(250),
      medium_info,
      read_snapshot,
      1_s/*timeout_us*/,
      &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(250, medium_info.medium_snapshot_);

  medium_info.reset();
  ret = tablet->get_snapshot<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      compaction::ObMediumCompactionInfoKey(270),
      medium_info,
      read_snapshot,
      1_s/*timeout_us*/,
      &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(270, medium_info.medium_snapshot_);

  medium_info.reset();
  ret = ut_scan_mds_key_at_snapshot_version(
      allocator_, *tablet, LS_ID, tablet_id, tablet->get_reorganization_scn(),
      compaction::ObMediumCompactionInfoKey(250), 250, 1_s, medium_info);
  ASSERT_EQ(OB_EMPTY_RESULT, ret);
}


TEST_F(TestMdsTableScan, test_range_cross)
{
  int ret = OB_SUCCESS;

  // create tablet
  const common::ObTabletID tablet_id(ObTimeUtility::fast_current_time() % 10000000000000);
  ObTabletHandle tablet_handle;
  ObTabletStatus status(ObTabletStatus::MAX);
  share::SCN create_commit_scn;
  create_commit_scn = share::SCN::plus(share::SCN::min_scn(), 100);
  ret = create_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTablet *tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
  share::SCN mds_checkpoint_scn = tablet->get_clog_checkpoint_scn();
  ASSERT_EQ(share::SCN::base_scn(), mds_checkpoint_scn);

  // write data to mds table no.1 row via replay
  {
    ObLSHandle ls_handle;
    ObTabletCreateDeleteMdsUserData user_data;
    share::SCN replay_scn;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, build_test_create_tablet_mds_user_data(
        ObTabletStatus::NORMAL, create_commit_scn, share::SCN::invalid_scn(), user_data));
    ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
        *ls_handle.get_ls(), create_commit_scn, replay_scn));
    ASSERT_EQ(OB_SUCCESS, ut_replay_tablet_status_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, replay_scn, transaction::ObTransID(123)));
  }

  // write data to mds table no.2 row via replay
  {
    ObLSHandle ls_handle;
    ObTabletBindingMdsUserData user_data;
    user_data.data_tablet_id_ = 100;
    user_data.hidden_tablet_id_ = 101;
    user_data.snapshot_version_ = 9527;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 200);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_EQ(OB_SUCCESS, ut_replay_ddl_info_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, commit_scn, transaction::ObTransID(256)));
  }

  // flush LS aggregate MDS once
  {
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = ls_handle.get_ls()->flush_mds_table();
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait all mds nodes to be released
  tablet_handle.reset();
  ret = wait_for_all_mds_nodes_released(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);

  // snapshot first flush result on LS_MDS_TABLET (minor or memtable)
  ObTabletHandle ls_mds_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ASSERT_EQ(OB_SUCCESS, TestMdsTableScan::get_ls_mds_tablet(ls_mds_handle));
  ret = ls_mds_handle.get_obj()->fetch_table_store(table_store_wrapper);
  EXPECT_EQ(OB_SUCCESS, ret);
  const ObTabletTableStore *table_store = table_store_wrapper.get_member();
  ObSEArray<ObITable *, 8> memtables;
  EXPECT_EQ(OB_SUCCESS, ls_mds_handle.get_obj()->inner_get_memtables(memtables));
  EXPECT_GT(memtables.count(), 0);

  // write data to mds table no.3 row via replay
  {
    ObLSHandle ls_handle;
    compaction::ObMediumCompactionInfoKey key(250);
    compaction::ObMediumCompactionInfo info;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 270);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info, 250);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, ut_replay_keyed_mds_for_test(
        *ls_handle.get_ls(), tablet_id, key, info, commit_scn, transaction::ObTransID(300)));
  }
  // write data to mds table no.4 row via replay
  {
    ObLSHandle ls_handle;
    compaction::ObMediumCompactionInfoKey key(270);
    compaction::ObMediumCompactionInfo info;
    const share::SCN commit_scn = share::SCN::plus(share::SCN::min_scn(), 290);
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = MediumInfoHelper::build_medium_compaction_info(allocator_, info, 270);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, ut_replay_keyed_mds_for_test(
        *ls_handle.get_ls(), tablet_id, key, info, commit_scn, transaction::ObTransID(320)));
  }

  share::SCN transfer_out_commit_scn = share::SCN::plus(share::SCN::min_scn(), 300);
  // write data to mds table no.5 row via replay
  {
    ObLSHandle ls_handle;
    ObTabletCreateDeleteMdsUserData user_data;
    share::SCN replay_scn;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    build_test_transfer_out_tablet_status(transfer_out_commit_scn, user_data);
    ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
        *ls_handle.get_ls(), transfer_out_commit_scn, replay_scn));
    ASSERT_EQ(OB_SUCCESS, ut_replay_tablet_status_for_test(
        *ls_handle.get_ls(), tablet_id, user_data, replay_scn, transaction::ObTransID(356)));
  }

  // flush LS aggregate MDS again (hope to build minor sstable)
  {
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->get_ls(LS_ID, ls_handle, ObLSGetMod::STORAGE_MOD));
    ret = ls_handle.get_ls()->flush_mds_table();
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait for mds table flush except->triggle minor
  ret = wait_for_mds_table_flush(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // wait all mds nodes to be released
  tablet_handle.reset();
  ret = wait_for_all_mds_nodes_released(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);

  // schedule mds minor on LS_MDS_TABLET
  (void)try_schedule_mds_minor(tablet_id);

  ret = TestMdsTableScan::get_tablet(tablet_id, tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  tablet = tablet_handle.get_obj();
  ASSERT_NE(nullptr, tablet);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_mds_table_scan.log*");
  OB_LOGGER.set_file_name("test_mds_table_scan.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

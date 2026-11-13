/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#include <functional>
#include <memory>
#include <gtest/gtest.h>

#define private public
#define protected public
#include "storage/ls/ob_ls.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/tx_table/ob_tx_data_cache.h"
#include "storage/tx/ob_trans_ctx_mgr_v4.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "lib/alloc/ob_malloc_allocator.h"

namespace oceanbase {
namespace transaction {
// Keep this seam below the tx-table unity build's inlined context lookup.
int ObLSTxCtxMgr::check_with_tx_data(const ObTransID &, storage::ObITxDataCheckFunctor &)
{ return OB_TRANS_CTX_NOT_EXIST; }
}
namespace storage {
// Only the storage/lifecycle dependencies are replaced. The request, cache
// insertion, final epoch check, and ObTxTable offline/online code are real.
int ObTxTable::offline_tx_ctx_table_() { return OB_SUCCESS; }
int ObTxTable::load_tx_ctx_table_() { return OB_SUCCESS; }
int ObTxDataTable::offline() { return OB_SUCCESS; }
int ObTxDataTable::online() { return OB_SUCCESS; }
}

namespace unittest {
using namespace common;
using namespace share;
using namespace storage;
using namespace transaction;

class PausingTxDataTable : public ObTxDataTable
{
public:
  int check_with_tx_data(const ObTransID tx_id, ObITxDataCheckFunctor &fn,
                        ObTxDataGuard &guard, SCN &recycled_scn) override
  {
    ++read_count_;
    recycled_scn.set_min();
    int ret = guard.init(data_);
    if (OB_SUCCESS == ret) {
      ret = fn(*guard.tx_data());
      if (OB_SUCCESS == ret && after_read_) {
        after_read_();
      }
    }
    return ret;
  }
  ObTxData *data_ = nullptr;
  int read_count_ = 0;
  std::function<void()> after_read_;
};

class TestTxTableCacheEpoch : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS,
        lib::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(1002));
  }
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
    ASSERT_EQ(OB_SUCCESS, getter_.add_tenant(1002, 8L << 20, 64L << 20));
    ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().init(
        &getter_, 1024, 1L << 30, lib::ACHUNK_SIZE));
    ASSERT_EQ(OB_SUCCESS, OB_TX_DATA_KV_CACHE.init("tx_data_kv_cache", 2));
  }
  void TearDown() override
  {
    OB_TX_DATA_KV_CACHE.destroy();
    ObKVGlobalCache::get_instance().destroy();
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }
  ObSimpleMemLimitGetter getter_;
};

TEST_F(TestTxTableCacheEpoch, production_read_late_put_uses_captured_epoch)
{
  ObTenantBase tenant(1002);
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(&tenant));
  std::unique_ptr<ObLS> ls(new ObLS());
  ls->ls_meta_.set_ls_epoch(100);
  TxShareThrottleTool throttle;
  ObTenantTxDataAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init("TxEpochUT", &throttle));
  void *memory = allocator.alloc(false /*enable_throttle*/);
  ASSERT_NE(nullptr, memory);
  ObTxData *data = new (memory) ObTxData();
  data->tx_data_allocator_ = &allocator;
  data->tx_id_ = ObTransID(12345);
  ASSERT_FALSE(data->tx_id_.is_reserved_id());
  data->state_ = ObTxData::COMMIT;
  ASSERT_EQ(OB_SUCCESS, data->commit_version_.convert_for_tx(160));
  ASSERT_EQ(OB_SUCCESS, data->end_scn_.convert_for_tx(200));
  ObTxDataGuard owner;
  ASSERT_EQ(OB_SUCCESS, owner.init(data));
  PausingTxDataTable tx_data_table;
  tx_data_table.data_ = data;
  ObTxTable table(tx_data_table);
  table.ls_ = ls.get();
  table.ls_id_ = ObLSID(1);
  table.is_inited_ = true;
  table.epoch_ = 0;
  ASSERT_EQ(OB_SUCCESS, table.online());
  ObTxTableGuard old_guard;
  ASSERT_EQ(OB_SUCCESS, table.get_tx_table_guard(old_guard));
  const int64_t old_epoch = table.get_epoch();

  // This callback runs after the production request has captured its epoch
  // and resolved TxData, but before check_tx_data_in_tables_ publishes it.
  tx_data_table.after_read_ = [&]() {
    EXPECT_EQ(OB_SUCCESS, table.offline());
    EXPECT_EQ(OB_SUCCESS, table.online());
  };
  int64_t state = -1;
  SCN version;
  SCN end_scn;
  EXPECT_EQ(OB_REPLICA_NOT_READABLE, old_guard.get_tx_state_with_scn(
      data->tx_id_, SCN::max_scn(), state, version, end_scn));
  EXPECT_EQ(1, tx_data_table.read_count_);
  ASSERT_EQ(old_epoch + 1, table.get_epoch());
  tx_data_table.after_read_ = nullptr;

  ObTxDataValueHandle value;
  const ObTxDataCacheKey old_key(1002, table.ls_id_, 100, old_epoch, data->tx_id_);
  const ObTxDataCacheKey new_key(1002, table.ls_id_, 100, table.get_epoch(), data->tx_id_);
  ASSERT_EQ(OB_SUCCESS, OB_TX_DATA_KV_CACHE.get_row(old_key, value));
  value.handle_.reset();
  value.value_ = nullptr;
  // This fails if the production put argument is changed to current epoch_.
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, OB_TX_DATA_KV_CACHE.get_row(new_key, value));

  ObTxTableGuard new_guard;
  ASSERT_EQ(OB_SUCCESS, table.get_tx_table_guard(new_guard));
  ASSERT_EQ(OB_SUCCESS, new_guard.get_tx_state_with_scn(
      data->tx_id_, SCN::max_scn(), state, version, end_scn));
  EXPECT_EQ(ObTxData::COMMIT, state);
  EXPECT_EQ(160, version.get_val_for_tx());
  EXPECT_EQ(2, tx_data_table.read_count_);
  ASSERT_EQ(OB_SUCCESS, OB_TX_DATA_KV_CACHE.get_row(new_key, value));
  value.handle_.reset();
  value.value_ = nullptr;
  // A fresh guard has no mini-cache; this read must hit the new KV generation.
  ObTxTableGuard cached_guard;
  ASSERT_EQ(OB_SUCCESS, table.get_tx_table_guard(cached_guard));
  ASSERT_EQ(OB_SUCCESS, cached_guard.get_tx_state_with_scn(
      data->tx_id_, SCN::max_scn(), state, version, end_scn));
  EXPECT_EQ(2, tx_data_table.read_count_);
}
}
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  oceanbase::common::ObLogger::get_logger().set_file_name("test_tx_table_cache_epoch.log", true);
  oceanbase::common::ObLogger::get_logger().set_enable_async_log(false);
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  return RUN_ALL_TESTS();
}

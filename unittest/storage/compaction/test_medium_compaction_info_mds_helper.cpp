/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include <gtest/gtest.h>
#define private public
#define protected public
#include "storage/compaction/ob_medium_compaction_info_mds_helper.h"
#include "storage/compaction/ob_medium_list_checker.h"
#include "storage/compaction/ob_tablet_medium_clog_replay_executor.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/compaction/ob_tablet_merge_task.h"
#include "unittest/storage/test_schema_prepare.h"
#include "unittest/storage/test_dml_common.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/init_basic_struct.h"
#include "unittest/storage/test_tablet_helper.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/multi_data_source/runtime_utility/mds_factory.h"
#include "storage/tx/ob_multi_data_source.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"

namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace storage;
using namespace share;

namespace unittest
{

static constexpr uint64_t TEST_LEGACY_SOURCE_TENANT_ID = 1002;

class MediumCompactionMdsTestUtil
{
public:
  void reset()
  {
    allocator_.reset();
  }

  void set_basic_medium_info(ObMediumCompactionInfo &medium_info, const int64_t medium_snapshot)
  {
    share::schema::ObTableSchema table_schema;
    TestSchemaPrepare::prepare_schema(table_schema);
    medium_info.compaction_type_ = ObMediumCompactionInfo::MEDIUM_COMPACTION;
    medium_info.medium_snapshot_ = medium_snapshot;
    medium_info.last_medium_snapshot_ = medium_snapshot - 100;
    medium_info.data_version_ = 100;
    medium_info.medium_compat_version_ = ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_V2;
    medium_info.cluster_id_ = GCONF.cluster_id;
    medium_info.tenant_id_ = 1;
    EXPECT_EQ(OB_SUCCESS, medium_info.storage_schema_.init(allocator_, table_schema, lib::Worker::CompatMode::MYSQL));
  }

  int build_item(
      const ObTabletID &tablet_id,
      const int64_t medium_snapshot,
      ObTabletMediumCompactionInfoMdsItem &item)
  {
    int ret = OB_SUCCESS;
    item.reset();
    item.tablet_id_ = tablet_id;
    set_basic_medium_info(item.medium_info_, medium_snapshot);
    return ret;
  }

  int build_arg(
      const ObLSID &ls_id,
      const ObIArray<ObTabletID> &tablet_ids,
      const ObIArray<int64_t> &medium_snapshots,
      ObTabletMediumCompactionInfoMdsArg &arg)
  {
    int ret = OB_SUCCESS;
    arg.reset();
    if (tablet_ids.count() != medium_snapshots.count()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      arg.ls_id_ = ls_id;
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
        ObMediumCompactionInfo medium_info;
        set_basic_medium_info(medium_info, medium_snapshots.at(i));
        if (OB_FAIL(arg.add_item(tablet_ids.at(i), medium_info))) {
          LOG_WARN("failed to add item", KR(ret), K(i));
        }
      }
    }
    return ret;
  }

  ObArenaAllocator &get_allocator() { return allocator_; }

private:
  ObArenaAllocator allocator_;
};

class TestObMediumCompactionInfoMdsHelper : public ::testing::Test
{
public:
  virtual void SetUp()
  {
    GCONF.cluster_id = 1;
    util_.reset();
  }
  virtual void TearDown()
  {
    util_.reset();
  }

protected:
  MediumCompactionMdsTestUtil util_;
};

static int ut_prepare_create_tablet_mds_ctx(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    mds::MdsCtx &ctx,
    memtable::ObMemtableCtx &mem_ctx,
    ObStoreCtx &store_ctx,
    const bool for_replay,
    const transaction::ObTransID &tx_id = transaction::ObTransID(1))
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle mds_tablet_hdl;
  ObLS *ls = nullptr;
  if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "failed to get ls for create tablet mds ctx", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null for create tablet mds ctx", K(ret), K(ls_id));
  } else if (OB_FAIL(ensure_mds_type_registry_for_ut())) {
    STORAGE_LOG(WARN, "failed to ensure mds type registry for create tablet test", K(ret), K(ls_id));
  } else if (OB_FAIL(ensure_ls_mds_tablet_memtable_ready(*ls))) {
    STORAGE_LOG(WARN, "failed to ensure ls mds tablet memtable", K(ret), K(ls_id));
  } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet(LS_MDS_TABLET, mds_tablet_hdl))) {
    STORAGE_LOG(WARN, "failed to get ls mds tablet", K(ret), K(ls_id));
  } else if (for_replay) {
    if (OB_FAIL(mem_ctx.init(tenant_id, CLUSTER_CURRENT_VERSION, tx_id, ls_id, common::ObAddr()))) {
      STORAGE_LOG(WARN, "failed to init mem_ctx for create tablet replay", K(ret), K(ls_id));
    } else {
      ctx.set_mem_ctx_for_replay(mem_ctx);
    }
  } else if (OB_FAIL(TestTabletHelper::prepare_mds_write_ctx_for_test(
      *ls, *mds_tablet_hdl.get_obj(), LS_MDS_TABLET, tx_id, ctx, mem_ctx, store_ctx))) {
    STORAGE_LOG(WARN, "failed to prepare store ctx for create tablet register", K(ret), K(ls_id));
  }
  return ret;
}

static int ut_finish_mds_register(
    mds::BufferCtx &ctx,
    memtable::ObMemtableCtx &mem_ctx,
    const share::SCN &commit_scn)
{
  return ut_finish_mds_tx_commit(static_cast<mds::MdsCtx&>(ctx), mem_ctx, commit_scn);
}

TEST_F(TestObMediumCompactionInfoMdsHelper, test_item_is_valid)
{
  ObTabletMediumCompactionInfoMdsItem item(util_.get_allocator());
  ASSERT_FALSE(item.is_valid());

  ASSERT_EQ(OB_SUCCESS, util_.build_item(ObTabletID(1001), 300, item));
  ASSERT_TRUE(item.is_valid());
  ASSERT_TRUE(item.tablet_id_.is_valid());
  ASSERT_TRUE(item.medium_info_.is_valid());
}

TEST_F(TestObMediumCompactionInfoMdsHelper, test_arg_is_valid)
{
  ObTabletMediumCompactionInfoMdsArg arg;
  ASSERT_FALSE(arg.is_valid());
  ASSERT_EQ(0, arg.get_tablet_count());

  ObSEArray<ObTabletID, 4> tablet_ids;
  ObSEArray<int64_t, 4> medium_snapshots;
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(ObTabletID(1001)));
  ASSERT_EQ(OB_SUCCESS, medium_snapshots.push_back(300));
  ASSERT_EQ(OB_SUCCESS, util_.build_arg(ObLSID(1001), tablet_ids, medium_snapshots, arg));
  ASSERT_TRUE(arg.is_valid());
  ASSERT_EQ(1, arg.get_tablet_count());
}

TEST_F(TestObMediumCompactionInfoMdsHelper, test_item_serialize_deserialize)
{
  ObTabletMediumCompactionInfoMdsItem src_item(util_.get_allocator());
  ASSERT_EQ(OB_SUCCESS, util_.build_item(ObTabletID(2001), 500, src_item));

  const int64_t buf_len = src_item.get_serialize_size();
  char *buf = static_cast<char *>(util_.get_allocator().alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src_item.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObTabletMediumCompactionInfoMdsItem dst_item(util_.get_allocator());
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst_item.deserialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);
  ASSERT_TRUE(dst_item.is_valid());
  ASSERT_EQ(src_item.tablet_id_, dst_item.tablet_id_);
  ASSERT_EQ(src_item.medium_info_.medium_snapshot_, dst_item.medium_info_.medium_snapshot_);
  ASSERT_EQ(src_item.medium_info_.last_medium_snapshot_, dst_item.medium_info_.last_medium_snapshot_);
}

TEST_F(TestObMediumCompactionInfoMdsHelper, test_arg_serialize_deserialize_single_item)
{
  ObSEArray<ObTabletID, 4> tablet_ids;
  ObSEArray<int64_t, 4> medium_snapshots;
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(ObTabletID(3001)));
  ASSERT_EQ(OB_SUCCESS, medium_snapshots.push_back(700));

  ObTabletMediumCompactionInfoMdsArg src_arg;
  ASSERT_EQ(OB_SUCCESS, util_.build_arg(ObLSID(1001), tablet_ids, medium_snapshots, src_arg));
  src_arg.unused_ = TEST_LEGACY_SOURCE_TENANT_ID;
  ASSERT_TRUE(src_arg.is_valid());

  const int64_t buf_len = src_arg.get_serialize_size();
  char *buf = static_cast<char *>(util_.get_allocator().alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src_arg.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObTabletMediumCompactionInfoMdsArg dst_arg;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst_arg.deserialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);
  ASSERT_TRUE(dst_arg.is_valid());
  ASSERT_EQ(src_arg.unused_, dst_arg.unused_);
  ASSERT_EQ(src_arg.ls_id_, dst_arg.ls_id_);
  ASSERT_EQ(1, dst_arg.get_tablet_count());
  ASSERT_EQ(src_arg.items_.at(0)->tablet_id_, dst_arg.items_.at(0)->tablet_id_);
  ASSERT_EQ(
      src_arg.items_.at(0)->medium_info_.medium_snapshot_,
      dst_arg.items_.at(0)->medium_info_.medium_snapshot_);
}

TEST_F(TestObMediumCompactionInfoMdsHelper, test_arg_serialize_deserialize_batch_items)
{
  ObSEArray<ObTabletID, 4> tablet_ids;
  ObSEArray<int64_t, 4> medium_snapshots;
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(ObTabletID(4001)));
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(ObTabletID(4002)));
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(ObTabletID(4003)));
  ASSERT_EQ(OB_SUCCESS, medium_snapshots.push_back(800));
  ASSERT_EQ(OB_SUCCESS, medium_snapshots.push_back(900));
  ASSERT_EQ(OB_SUCCESS, medium_snapshots.push_back(1000));

  ObTabletMediumCompactionInfoMdsArg src_arg;
  ASSERT_EQ(OB_SUCCESS, util_.build_arg(ObLSID(1001), tablet_ids, medium_snapshots, src_arg));
  src_arg.unused_ = TEST_LEGACY_SOURCE_TENANT_ID;
  ASSERT_EQ(3, src_arg.get_tablet_count());

  const int64_t buf_len = src_arg.get_serialize_size();
  char *buf = static_cast<char *>(util_.get_allocator().alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src_arg.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObTabletMediumCompactionInfoMdsArg dst_arg;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst_arg.deserialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);
  ASSERT_EQ(src_arg.unused_, dst_arg.unused_);
  ASSERT_EQ(3, dst_arg.get_tablet_count());
  for (int64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(src_arg.items_.at(i)->tablet_id_, dst_arg.items_.at(i)->tablet_id_);
    ASSERT_EQ(
        src_arg.items_.at(i)->medium_info_.medium_snapshot_,
        dst_arg.items_.at(i)->medium_info_.medium_snapshot_);
  }
}

TEST_F(TestObMediumCompactionInfoMdsHelper, test_arg_deserialize_zero_item_count)
{
  ObTabletMediumCompactionInfoMdsArg src_arg;
  src_arg.ls_id_ = ObLSID(1001);

  const int64_t buf_len = src_arg.get_serialize_size();
  char *buf = static_cast<char *>(util_.get_allocator().alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src_arg.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObTabletMediumCompactionInfoMdsArg arg;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, arg.deserialize(buf, buf_len, pos));
  ASSERT_FALSE(arg.is_valid());
}

class TestObMediumCompactionInfoMdsHelperMdsRegister : public ::testing::Test
{
public:
  static constexpr uint64_t TEST_TENANT_ID = 1;
  static constexpr int64_t TEST_LS_ID = 1001;
  static constexpr int64_t TEST_TABLET_ID_SINGLE = 30001;
  static constexpr int64_t TEST_TABLET_ID_BATCH_1 = 30011;
  static constexpr int64_t TEST_TABLET_ID_BATCH_2 = 30012;
  static constexpr int64_t TEST_TABLET_ID_REPLAY = 30021;
  static constexpr int64_t TEST_TABLET_ID_RESTORE_REPLAY = 30022;
  static constexpr int64_t TEST_TABLET_ID_NOT_EXIST = 39999;
  static constexpr int64_t TEST_MEDIUM_SNAPSHOT_1 = 300;
  static constexpr int64_t TEST_MEDIUM_SNAPSHOT_2 = 400;
  static constexpr int64_t TEST_MEDIUM_SNAPSHOT_3 = 500;
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    SERVER_STORAGE_META_SERVICE.is_started_ = true;
    ObLSHandle ls_handle;
    ASSERT_EQ(OB_SUCCESS, TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle));
  }
  static void TearDownTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID)));
    MockTenantModuleEnv::get_instance().destroy();
  }
  virtual void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
    GCONF.cluster_id = 1;
  }
  virtual void TearDown()
  {
    util_.reset();
    allocator_.reset();
  }

  int prepare_ls(ObLSHandle &ls_handle)
  {
    int ret = OB_SUCCESS;
    const ObLSID ls_id(TEST_LS_ID);
    ObLSService *ls_svr = MTL(ObLSService*);
    if (OB_ISNULL(ls_svr)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls", KR(ret), K(ls_id));
    }
    return ret;
  }

  int prepare_tablet(
      ObLSHandle &ls_handle,
      const ObTabletID &tablet_id)
  {
    int ret = OB_SUCCESS;
    share::schema::ObTableSchema table_schema;
    if (OB_FAIL(build_test_schema(table_schema, 12345))) {
      LOG_WARN("failed to build test schema", KR(ret));
    } else if (OB_FAIL(TestTabletHelper::create_tablet(
        ls_handle, tablet_id, table_schema, allocator_))) {
      LOG_WARN("failed to create tablet", KR(ret), K(tablet_id));
    }
    return ret;
  }

  int serialize_arg_to_buf(
      const ObTabletMediumCompactionInfoMdsArg &arg,
      char *&buf,
      int64_t &buf_len)
  {
    int ret = OB_SUCCESS;
    buf = nullptr;
    buf_len = arg.get_serialize_size();
    int64_t pos = 0;
    if (buf_len <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(arg.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize arg", KR(ret), K(arg));
    } else if (pos != buf_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected serialize size", KR(ret), K(pos), K(buf_len));
    }
    return ret;
  }

  bool has_medium_snapshot(
      const ObMediumCompactionInfoList *medium_list,
      const int64_t medium_snapshot) const
  {
    bool found = false;
    if (OB_NOT_NULL(medium_list)) {
      DLIST_FOREACH_NORET(info, medium_list->get_list()) {
        if (OB_NOT_NULL(info) && info->medium_snapshot_ == medium_snapshot) {
          found = true;
          break;
        }
      }
    }
    return found;
  }

  int get_medium_snapshot_cnt(
      const ObMediumCompactionInfoList *medium_list,
      int64_t &cnt) const
  {
    cnt = 0;
    if (OB_ISNULL(medium_list)) {
      return OB_INVALID_ARGUMENT;
    }
    cnt = medium_list->size();
    return OB_SUCCESS;
  }

  void destroy_mds_ctx(mds::BufferCtx *&ctx)
  {
    if (OB_NOT_NULL(ctx)) {
      mds::BufferCtxNode node;
      node.set_ctx(ctx);
      node.destroy_ctx();
      ctx = nullptr;
    }
  }

  // Drive the same row callbacks as follower REDO followed by COMMIT/ABORT.
  // BufferCtx::on_abort alone does not roll back aggregate MDS MemTable rows.
  int replay_and_end_transaction(
      const ObTabletMediumCompactionInfoMdsArg &arg,
      const transaction::ObTransID &tx_id,
      const share::SCN &redo_scn,
      const share::SCN &end_scn,
      const bool commit)
  {
    int ret = OB_SUCCESS;
    char *buf = nullptr;
    int64_t buf_len = 0;
    mds::BufferCtx *ctx = nullptr;
    memtable::ObMemtableCtx mem_ctx;
    ObStoreCtx store_ctx;
    if (OB_FAIL(serialize_arg_to_buf(arg, buf, buf_len))) {
      LOG_WARN("failed to serialize medium info for replay test", KR(ret));
    } else if (OB_FAIL(mds::MdsFactory::create_buffer_ctx(
        transaction::ObTxDataSourceType::MEDIUM_COMPACTION_INFO, tx_id, ctx))) {
      LOG_WARN("failed to create replay test context", KR(ret));
    } else if (OB_FAIL(ut_prepare_create_tablet_mds_ctx(
        TEST_TENANT_ID, arg.ls_id_, static_cast<mds::MdsCtx&>(*ctx),
        mem_ctx, store_ctx, true /*for_replay*/, tx_id))) {
      LOG_WARN("failed to prepare replay test context", KR(ret));
    } else {
      ctx->set_for_replay(true);
      static_cast<mds::MdsCtx&>(*ctx).set_seq_no(transaction::ObTxSEQ(1, 0));
      ret = ObTabletMediumCompactionInfoMdsHelper::on_replay(buf, buf_len, redo_scn, *ctx);
      if (OB_SUCC(ret)) {
        ctx->on_redo(redo_scn);
      }
      const bool do_commit = OB_SUCC(ret) && commit;
      const SCN commit_version = do_commit ? end_scn : SCN::min_scn();
      const int end_ret = mem_ctx.trans_replay_end(do_commit, commit_version, end_scn);
      if (OB_SUCCESS != end_ret) {
        LOG_WARN("failed to end replay test transaction", K(end_ret), K(do_commit));
        ret = OB_SUCC(ret) ? end_ret : ret;
      } else if (do_commit) {
        ctx->on_commit(commit_version, end_scn);
      } else {
        ctx->on_abort(end_scn);
      }
    }
    destroy_mds_ctx(ctx);
    return ret;
  }

protected:
  MediumCompactionMdsTestUtil util_;

private:
  ObArenaAllocator allocator_;
};

TEST_F(TestObMediumCompactionInfoMdsHelperMdsRegister, test_mds_create_buffer_ctx)
{
  static int64_t TX_ID = 1;
  mds::BufferCtx *ctx = nullptr;
  memtable::ObMemtableCtx mem_ctx;
  ObStoreCtx store_ctx;
  ASSERT_EQ(OB_SUCCESS, mds::MdsFactory::create_buffer_ctx(
    transaction::ObTxDataSourceType::MEDIUM_COMPACTION_INFO,
    transaction::ObTransID(TX_ID++),
    ctx));
  ASSERT_EQ(OB_SUCCESS, ut_prepare_create_tablet_mds_ctx(TEST_TENANT_ID, ObLSID(TEST_LS_ID), static_cast<mds::MdsCtx&>(*ctx), mem_ctx, store_ctx, false));
  ASSERT_NE(nullptr, ctx);
  destroy_mds_ctx(ctx);
}

TEST_F(TestObMediumCompactionInfoMdsHelperMdsRegister, test_on_register_invalid_args)
{
  mds::BufferCtx *ctx = nullptr;
  char buf[16] = {0};
  memtable::ObMemtableCtx mem_ctx;
  ObStoreCtx store_ctx;
  ASSERT_EQ(OB_SUCCESS, mds::MdsFactory::create_buffer_ctx(
    transaction::ObTxDataSourceType::MEDIUM_COMPACTION_INFO,
    transaction::ObTransID(1),
    ctx));
  ASSERT_EQ(OB_SUCCESS, ut_prepare_create_tablet_mds_ctx(TEST_TENANT_ID, ObLSID(TEST_LS_ID), static_cast<mds::MdsCtx&>(*ctx), mem_ctx, store_ctx, false));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObTabletMediumCompactionInfoMdsHelper::on_register(nullptr, 10, *ctx));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObTabletMediumCompactionInfoMdsHelper::on_register(buf, 0, *ctx));
  destroy_mds_ctx(ctx);
}

TEST_F(TestObMediumCompactionInfoMdsHelperMdsRegister, test_on_replay_invalid_args)
{
  mds::BufferCtx *ctx = nullptr;
  char buf[16] = {0};
  memtable::ObMemtableCtx mem_ctx;
  ObStoreCtx store_ctx;
  ASSERT_EQ(OB_SUCCESS, mds::MdsFactory::create_buffer_ctx(
    transaction::ObTxDataSourceType::MEDIUM_COMPACTION_INFO,
    transaction::ObTransID(1),
    ctx));
  ASSERT_EQ(OB_SUCCESS, ut_prepare_create_tablet_mds_ctx(TEST_TENANT_ID, ObLSID(TEST_LS_ID), static_cast<mds::MdsCtx&>(*ctx), mem_ctx, store_ctx, true));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObTabletMediumCompactionInfoMdsHelper::on_replay(
      buf, sizeof(buf), share::SCN(), *ctx));
  destroy_mds_ctx(ctx);
}

TEST_F(TestObMediumCompactionInfoMdsHelperMdsRegister, test_on_register_tablet_not_exist)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 2> tablet_ids;
  ObSEArray<int64_t, 2> medium_snapshots;
  ObTabletMediumCompactionInfoMdsArg arg;
  char *buf = nullptr;
  int64_t buf_len = 0;
  mds::BufferCtx *ctx = nullptr;

  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(ObTabletID(TEST_TABLET_ID_NOT_EXIST)));
  ASSERT_EQ(OB_SUCCESS, medium_snapshots.push_back(TEST_MEDIUM_SNAPSHOT_1));
  ASSERT_EQ(OB_SUCCESS, util_.build_arg(ObLSID(TEST_LS_ID), tablet_ids, medium_snapshots, arg));
  ASSERT_EQ(OB_SUCCESS, serialize_arg_to_buf(arg, buf, buf_len));
  memtable::ObMemtableCtx mem_ctx;
  ObStoreCtx store_ctx;
  ASSERT_EQ(OB_SUCCESS, mds::MdsFactory::create_buffer_ctx(
    transaction::ObTxDataSourceType::MEDIUM_COMPACTION_INFO,
    transaction::ObTransID(1),
    ctx));
  ASSERT_EQ(OB_SUCCESS, ut_prepare_create_tablet_mds_ctx(TEST_TENANT_ID, ObLSID(TEST_LS_ID), static_cast<mds::MdsCtx&>(*ctx), mem_ctx, store_ctx, false));
  ret = ObTabletMediumCompactionInfoMdsHelper::on_register(buf, buf_len, *ctx);
  ASSERT_NE(OB_SUCCESS, ret);
  destroy_mds_ctx(ctx);
}

TEST_F(TestObMediumCompactionInfoMdsHelperMdsRegister, test_on_register_single_tablet)
{
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObSEArray<ObTabletID, 2> tablet_ids;
  ObSEArray<int64_t, 2> medium_snapshots;
  ObTabletMediumCompactionInfoMdsArg arg;
  char *buf = nullptr;
  int64_t buf_len = 0;
  mds::BufferCtx *ctx = nullptr;
  const ObTabletID tablet_id(TEST_TABLET_ID_SINGLE);
  const ObLSID ls_id(TEST_LS_ID);

  ASSERT_EQ(OB_SUCCESS, prepare_ls(ls_handle));
  ASSERT_EQ(OB_SUCCESS, prepare_tablet(ls_handle, tablet_id));
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tablet_id));
  ASSERT_EQ(OB_SUCCESS, medium_snapshots.push_back(TEST_MEDIUM_SNAPSHOT_1));
  ASSERT_EQ(OB_SUCCESS, util_.build_arg(ls_id, tablet_ids, medium_snapshots, arg));
  ASSERT_EQ(OB_SUCCESS, serialize_arg_to_buf(arg, buf, buf_len));
  memtable::ObMemtableCtx mem_ctx;
  ObStoreCtx store_ctx;
  ASSERT_EQ(OB_SUCCESS, mds::MdsFactory::create_buffer_ctx(
    transaction::ObTxDataSourceType::MEDIUM_COMPACTION_INFO,
    transaction::ObTransID(1),
    ctx));
  ASSERT_EQ(OB_SUCCESS, ut_prepare_create_tablet_mds_ctx(TEST_TENANT_ID, ObLSID(TEST_LS_ID), static_cast<mds::MdsCtx&>(*ctx), mem_ctx, store_ctx, false));
  ASSERT_EQ(OB_SUCCESS, ObTabletMediumCompactionInfoMdsHelper::on_register(buf, buf_len, *ctx));
  share::SCN commit_scn;
  ASSERT_EQ(OB_SUCCESS, commit_scn.convert_for_tx(2001));
  ASSERT_EQ(OB_SUCCESS, ut_finish_mds_register(*ctx, mem_ctx, commit_scn));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  const ObMediumCompactionInfoList *medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
  ASSERT_NE(nullptr, medium_list);
  ASSERT_TRUE(has_medium_snapshot(medium_list, TEST_MEDIUM_SNAPSHOT_1));
  destroy_mds_ctx(ctx);
}

TEST_F(TestObMediumCompactionInfoMdsHelperMdsRegister, test_on_register_batch_tablets)
{
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle1;
  ObTabletHandle tablet_handle2;
  ObSEArray<ObTabletID, 4> tablet_ids;
  ObSEArray<int64_t, 4> medium_snapshots;
  ObTabletMediumCompactionInfoMdsArg arg;
  char *buf = nullptr;
  int64_t buf_len = 0;
  mds::BufferCtx *ctx = nullptr;
  const ObTabletID tablet_id1(TEST_TABLET_ID_BATCH_1);
  const ObTabletID tablet_id2(TEST_TABLET_ID_BATCH_2);
  const ObLSID ls_id(TEST_LS_ID);

  ASSERT_EQ(OB_SUCCESS, prepare_ls(ls_handle));
  ASSERT_EQ(OB_SUCCESS, prepare_tablet(ls_handle, tablet_id1));
  ASSERT_EQ(OB_SUCCESS, prepare_tablet(ls_handle, tablet_id2));
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tablet_id1));
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tablet_id2));
  ASSERT_EQ(OB_SUCCESS, medium_snapshots.push_back(TEST_MEDIUM_SNAPSHOT_1));
  ASSERT_EQ(OB_SUCCESS, medium_snapshots.push_back(TEST_MEDIUM_SNAPSHOT_2));
  ASSERT_EQ(OB_SUCCESS, util_.build_arg(ls_id, tablet_ids, medium_snapshots, arg));
  ASSERT_EQ(2, arg.get_tablet_count());
  ASSERT_EQ(OB_SUCCESS, serialize_arg_to_buf(arg, buf, buf_len));
  memtable::ObMemtableCtx mem_ctx;
  ObStoreCtx store_ctx;
  ASSERT_EQ(OB_SUCCESS, mds::MdsFactory::create_buffer_ctx(
    transaction::ObTxDataSourceType::MEDIUM_COMPACTION_INFO,
    transaction::ObTransID(1),
    ctx));
  ASSERT_EQ(OB_SUCCESS, ut_prepare_create_tablet_mds_ctx(TEST_TENANT_ID, ls_id, static_cast<mds::MdsCtx&>(*ctx), mem_ctx, store_ctx, false));
  ASSERT_EQ(OB_SUCCESS, ObTabletMediumCompactionInfoMdsHelper::on_register(buf, buf_len, *ctx));
  share::SCN commit_scn;
  ASSERT_EQ(OB_SUCCESS, commit_scn.convert_for_tx(2002));
  ASSERT_EQ(OB_SUCCESS, ut_finish_mds_register(*ctx, mem_ctx, commit_scn));

  const ObMediumCompactionInfoList *medium_list1 = nullptr;
  const ObMediumCompactionInfoList *medium_list2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id1, tablet_handle1));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id2, tablet_handle2));
  ASSERT_EQ(OB_SUCCESS, tablet_handle1.get_obj()->read_medium_info_list(allocator_, medium_list1));
  ASSERT_EQ(OB_SUCCESS, tablet_handle2.get_obj()->read_medium_info_list(allocator_, medium_list2));
  ASSERT_TRUE(has_medium_snapshot(medium_list1, TEST_MEDIUM_SNAPSHOT_1));
  ASSERT_TRUE(has_medium_snapshot(medium_list2, TEST_MEDIUM_SNAPSHOT_2));
  destroy_mds_ctx(ctx);
}

TEST_F(TestObMediumCompactionInfoMdsHelperMdsRegister, test_on_replay_single_tablet)
{
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObSEArray<ObTabletID, 2> tablet_ids;
  ObSEArray<int64_t, 2> medium_snapshots;
  ObTabletMediumCompactionInfoMdsArg arg;
  char *buf = nullptr;
  int64_t buf_len = 0;
  mds::BufferCtx *ctx = nullptr;
  const ObTabletID tablet_id(TEST_TABLET_ID_REPLAY);
  const ObLSID ls_id(TEST_LS_ID);
  share::SCN replay_scn;
  const ObMediumCompactionInfoList *medium_list = nullptr;
  int64_t medium_cnt = 0;

  ASSERT_EQ(OB_SUCCESS, replay_scn.convert_for_tx(1000));
  ASSERT_EQ(OB_SUCCESS, prepare_ls(ls_handle));
  ASSERT_EQ(OB_SUCCESS, prepare_tablet(ls_handle, tablet_id));
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tablet_id));
  ASSERT_EQ(OB_SUCCESS, medium_snapshots.push_back(TEST_MEDIUM_SNAPSHOT_2));
  ASSERT_EQ(OB_SUCCESS, util_.build_arg(ls_id, tablet_ids, medium_snapshots, arg));
  ASSERT_EQ(OB_SUCCESS, serialize_arg_to_buf(arg, buf, buf_len));
  memtable::ObMemtableCtx mem_ctx;
  ObStoreCtx store_ctx;
  ASSERT_EQ(OB_SUCCESS, mds::MdsFactory::create_buffer_ctx(
    transaction::ObTxDataSourceType::MEDIUM_COMPACTION_INFO,
    transaction::ObTransID(1),
    ctx));
  ASSERT_EQ(OB_SUCCESS, ut_prepare_create_tablet_mds_ctx(TEST_TENANT_ID, ls_id, static_cast<mds::MdsCtx&>(*ctx), mem_ctx, store_ctx, true));
  ASSERT_EQ(OB_SUCCESS, ObTabletMediumCompactionInfoMdsHelper::on_replay(buf, buf_len, replay_scn, *ctx));
  ASSERT_EQ(OB_SUCCESS, ut_finish_memctx_mds_replay(mem_ctx, replay_scn));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
  ASSERT_EQ(OB_SUCCESS, get_medium_snapshot_cnt(medium_list, medium_cnt));
  ASSERT_GE(medium_cnt, 1);
  ASSERT_TRUE(has_medium_snapshot(medium_list, TEST_MEDIUM_SNAPSHOT_2));
  destroy_mds_ctx(ctx);
}

TEST_F(TestObMediumCompactionInfoMdsHelperMdsRegister, test_on_replay_legacy_source_tenant)
{
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObSEArray<ObTabletID, 2> tablet_ids;
  ObSEArray<int64_t, 2> medium_snapshots;
  ObTabletMediumCompactionInfoMdsArg arg;
  char *buf = nullptr;
  int64_t buf_len = 0;
  mds::BufferCtx *ctx = nullptr;
  const ObTabletID tablet_id(TEST_TABLET_ID_RESTORE_REPLAY);
  const ObLSID ls_id(TEST_LS_ID);
  share::SCN replay_scn;
  const ObMediumCompactionInfoList *medium_list = nullptr;

  ASSERT_EQ(TEST_TENANT_ID, MTL_ID());
  ASSERT_NE(TEST_LEGACY_SOURCE_TENANT_ID, MTL_ID());
  ASSERT_EQ(OB_SUCCESS, replay_scn.convert_for_tx(1001));
  ASSERT_EQ(OB_SUCCESS, prepare_ls(ls_handle));
  ASSERT_EQ(OB_SUCCESS, prepare_tablet(ls_handle, tablet_id));
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tablet_id));
  ASSERT_EQ(OB_SUCCESS, medium_snapshots.push_back(TEST_MEDIUM_SNAPSHOT_3));
  ASSERT_EQ(OB_SUCCESS, util_.build_arg(ls_id, tablet_ids, medium_snapshots, arg));
  arg.unused_ = TEST_LEGACY_SOURCE_TENANT_ID;
  ASSERT_EQ(1, arg.get_tablet_count());
  ASSERT_NE(nullptr, arg.items_.at(0));
  arg.items_.at(0)->medium_info_.tenant_id_ = TEST_LEGACY_SOURCE_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, serialize_arg_to_buf(arg, buf, buf_len));
  memtable::ObMemtableCtx mem_ctx;
  ObStoreCtx store_ctx;
  ASSERT_EQ(OB_SUCCESS, mds::MdsFactory::create_buffer_ctx(
    transaction::ObTxDataSourceType::MEDIUM_COMPACTION_INFO,
    transaction::ObTransID(1),
    ctx));
  ASSERT_EQ(OB_SUCCESS, ut_prepare_create_tablet_mds_ctx(
      TEST_TENANT_ID, ls_id, static_cast<mds::MdsCtx&>(*ctx), mem_ctx, store_ctx, true));
  ASSERT_EQ(OB_SUCCESS, ObTabletMediumCompactionInfoMdsHelper::on_replay(buf, buf_len, replay_scn, *ctx));
  ASSERT_EQ(OB_SUCCESS, ut_finish_memctx_mds_replay(mem_ctx, replay_scn));

  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
  ASSERT_NE(nullptr, medium_list);
  ASSERT_TRUE(has_medium_snapshot(medium_list, TEST_MEDIUM_SNAPSHOT_3));
  destroy_mds_ctx(ctx);
}

TEST_F(TestObMediumCompactionInfoMdsHelperMdsRegister, test_on_replay_uses_transaction_commit_version)
{
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  const ObLSID ls_id(TEST_LS_ID);
  const ObTabletID tablet_id(30023);
  ObSEArray<ObTabletID, 2> tablet_ids;
  ObSEArray<int64_t, 2> medium_snapshots;
  ObTabletMediumCompactionInfoMdsArg arg;
  SCN redo_scn;
  ASSERT_EQ(OB_SUCCESS, prepare_ls(ls_handle));
  ASSERT_EQ(OB_SUCCESS, prepare_tablet(ls_handle, tablet_id));
  ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
      *ls_handle.get_ls(), SCN::min_scn(), redo_scn));
  redo_scn = SCN::plus(redo_scn, 1000);
  const SCN commit_scn = SCN::plus(redo_scn, 10);
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tablet_id));
  ASSERT_EQ(OB_SUCCESS, medium_snapshots.push_back(200));
  ASSERT_EQ(OB_SUCCESS, util_.build_arg(ls_id, tablet_ids, medium_snapshots, arg));
  ASSERT_EQ(OB_SUCCESS, replay_and_end_transaction(
      arg, transaction::ObTransID(101), redo_scn, commit_scn, true /*commit*/));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

  ObMediumCompactionInfo result;
  const ObMediumCompactionInfoKey key(200);
  // REDO must not publish a committed row at a snapshot before transaction commit.
  EXPECT_EQ(OB_EMPTY_RESULT, tablet_handle.get_obj()->get_mds_snapshot_data(
      key, result, SCN::minus(commit_scn, 1), 0 /*timeout_us*/, &allocator_));
  result.reset();
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->get_mds_snapshot_data(
      key, result, commit_scn, 0 /*timeout_us*/, &allocator_));
  EXPECT_EQ(200, result.medium_snapshot_);
}

TEST_F(TestObMediumCompactionInfoMdsHelperMdsRegister, test_on_replay_abort_then_retry_batch)
{
  ObLSHandle ls_handle;
  const ObLSID ls_id(TEST_LS_ID);
  const int64_t base_snapshot = 100;
  ObSEArray<ObTabletID, 2> tablet_ids;
  ObSEArray<int64_t, 2> medium_snapshots;
  ObTabletMediumCompactionInfoMdsArg arg;
  SCN redo_scn;
  ASSERT_EQ(OB_SUCCESS, prepare_ls(ls_handle));
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(ObTabletID(30024)));
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(ObTabletID(30025)));
  for (int64_t i = 0; i < tablet_ids.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, prepare_tablet(ls_handle, tablet_ids.at(i)));
    ASSERT_EQ(OB_SUCCESS, medium_snapshots.push_back(200));
  }
  ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
      *ls_handle.get_ls(), SCN::min_scn(), redo_scn));
  redo_scn = SCN::plus(redo_scn, 2000);
  ASSERT_EQ(OB_SUCCESS, util_.build_arg(ls_id, tablet_ids, medium_snapshots, arg));

  // T1 registers A: 100 -> 200 on both tablets; its REDO is followed by ABORT.
  ASSERT_EQ(OB_SUCCESS, replay_and_end_transaction(
      arg, transaction::ObTransID(102), redo_scn, SCN::plus(redo_scn, 10), false /*commit*/));
  for (int64_t i = 0; i < tablet_ids.count(); ++i) {
    ObTabletHandle tablet_handle;
    const ObMediumCompactionInfoList *medium_list = nullptr;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_ids.at(i), tablet_handle));
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
    ASSERT_NE(nullptr, medium_list);
    EXPECT_FALSE(has_medium_snapshot(medium_list, 200)) << "ABORT left a committed Medium Info";
    EXPECT_EQ(0, medium_list->size());
    ObMediumCompactionInfoList *allocated_list = const_cast<ObMediumCompactionInfoList*>(medium_list);
    ObTabletObjLoadHelper::free(allocator_, allocated_list);
  }

  // The leader rolled T1 back, so T2 legitimately retries B: 100 -> 300.
  medium_snapshots.reset();
  for (int64_t i = 0; i < tablet_ids.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, medium_snapshots.push_back(300));
  }
  ASSERT_EQ(OB_SUCCESS, util_.build_arg(ls_id, tablet_ids, medium_snapshots, arg));
  for (int64_t i = 0; i < arg.items_.count(); ++i) {
    arg.items_.at(i)->medium_info_.last_medium_snapshot_ = base_snapshot;
  }
  const SCN retry_scn = SCN::plus(redo_scn, 20);
  ASSERT_EQ(OB_SUCCESS, replay_and_end_transaction(
      arg, transaction::ObTransID(103), retry_scn, SCN::plus(retry_scn, 10), true /*commit*/));
  for (int64_t i = 0; i < tablet_ids.count(); ++i) {
    ObTabletHandle tablet_handle;
    ObSEArray<ObMediumCompactionInfo*, 2> medium_array;
    ObExtraMediumInfo extra_info;
    extra_info.last_medium_scn_ = base_snapshot;
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_ids.at(i), tablet_handle));
    ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->read_medium_array_(allocator_, medium_array));
    EXPECT_EQ(1, medium_array.count());
    EXPECT_EQ(OB_SUCCESS, ObMediumListChecker::validate_medium_info_list(
        extra_info, &medium_array, base_snapshot, 0 /*last_inc_major_snapshot*/,
        false /*allow_predecessor_mismatch*/))
        << "ABORT residue forks the Medium chain: 100->200 and 100->300";
    for (int64_t j = 0; j < medium_array.count(); ++j) {
      EXPECT_EQ(300, medium_array.at(j)->medium_snapshot_);
      ObTabletObjLoadHelper::free(allocator_, medium_array.at(j));
    }
  }
}

TEST_F(TestObMediumCompactionInfoMdsHelperMdsRegister, test_standalone_medium_clog_still_commits)
{
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  const ObLSID ls_id(TEST_LS_ID);
  const ObTabletID tablet_id(30026);
  SCN redo_scn;
  ASSERT_EQ(OB_SUCCESS, prepare_ls(ls_handle));
  ASSERT_EQ(OB_SUCCESS, prepare_tablet(ls_handle, tablet_id));
  ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
      *ls_handle.get_ls(), SCN::min_scn(), redo_scn));
  redo_scn = SCN::plus(redo_scn, 3000);
  ObMediumCompactionInfo medium_info;
  util_.set_basic_medium_info(medium_info, 200);
  ObTabletMediumClogReplayExecutor executor(medium_info);
  ASSERT_EQ(OB_SUCCESS, executor.init(redo_scn));
  ASSERT_EQ(OB_SUCCESS, executor.execute(redo_scn, ls_id, tablet_id));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  const ObMediumCompactionInfoList *medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
  ASSERT_NE(nullptr, medium_list);
  EXPECT_TRUE(has_medium_snapshot(medium_list, 200));
  ObMediumCompactionInfoList *allocated_list = const_cast<ObMediumCompactionInfoList*>(medium_list);
  ObTabletObjLoadHelper::free(allocator_, allocated_list);
}

TEST_F(TestObMediumCompactionInfoMdsHelperMdsRegister, test_deletion_gc_survives_medium_replay_and_retry)
{
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  const ObLSID ls_id(TEST_LS_ID);
  const ObTabletID tablet_id(30027);
  SCN redo_scn;
  ASSERT_EQ(OB_SUCCESS, prepare_ls(ls_handle));
  ASSERT_EQ(OB_SUCCESS, prepare_tablet(ls_handle, tablet_id));
  ASSERT_EQ(OB_SUCCESS, calc_test_tablet_mds_replay_scn(
      *ls_handle.get_ls(), SCN::min_scn(), redo_scn));
  redo_scn = SCN::plus(redo_scn, 3000);

  ObMediumCompactionInfo medium_info;
  util_.set_basic_medium_info(medium_info, 300);
  medium_info.medium_compat_version_ = ObMediumCompactionInfo::MEDIUM_COMPAT_VERSION_LATEST;
  medium_info.data_version_ = DATA_CURRENT_VERSION;
  medium_info.storage_schema_.index_type_ = schema::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL;
  ASSERT_EQ(OB_SUCCESS, medium_info.mds_filter_info_.init(medium_info.data_version_, 200));
  medium_info.contain_mds_filter_info_ = true;
  ASSERT_TRUE(medium_info.is_valid());
  const int64_t buf_len = medium_info.get_serialize_size();
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, medium_info.serialize(buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);
  ObMediumCompactionInfo replay_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, replay_info.deserialize(allocator_, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);
  ObTabletMediumClogReplayExecutor executor(replay_info);
  ASSERT_EQ(OB_SUCCESS, executor.init(redo_scn));
  ASSERT_EQ(OB_SUCCESS, executor.execute(redo_scn, ls_id, tablet_id));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  const ObMediumCompactionInfoList *medium_list = nullptr;
  ASSERT_EQ(OB_SUCCESS, tablet_handle.get_obj()->read_medium_info_list(allocator_, medium_list));
  ASSERT_NE(nullptr, medium_list);
  ObMediumCompactionInfoList *allocated_list = const_cast<ObMediumCompactionInfoList *>(medium_list);
  DEFER(ObTabletObjLoadHelper::free(allocator_, allocated_list));
  const ObMediumCompactionInfo *saved_info = medium_list->get_list().get_first();
  ASSERT_NE(nullptr, saved_info);
  ASSERT_EQ(300, saved_info->medium_snapshot_);
  ASSERT_TRUE(saved_info->contain_mds_filter_info_);
  ASSERT_EQ(200, saved_info->mds_filter_info_.get_mlog_purge_scn());

  // Reconstruct each attempt from the durable plan without an LS handle: filter
  // creation must not consult the current sibling tablets, even after recovery.
  for (int64_t attempt = 0; attempt < 2; ++attempt) {
    ObTabletMergeDagParam param;
    param.merge_type_ = MEDIUM_MERGE;
    param.merge_version_ = 300;
    param.ls_id_ = ls_id;
    param.tablet_id_ = tablet_id;
    ObTabletMergeCtx merge_ctx(param, allocator_);
    ObStorageSchema *storage_schema = OB_NEWx(ObStorageSchema, &allocator_);
    ASSERT_NE(nullptr, storage_schema);
    merge_ctx.static_param_.schema_ = storage_schema;
    ASSERT_EQ(OB_SUCCESS, storage_schema->init(allocator_, saved_info->storage_schema_));
    ASSERT_EQ(OB_SUCCESS, merge_ctx.filter_ctx_.mds_filter_info_.assign(
        allocator_, saved_info->mds_filter_info_));
    ASSERT_EQ(OB_SUCCESS, merge_ctx.alloc_mds_info_compaction_filter());
    ASSERT_NE(nullptr, merge_ctx.get_compaction_filter());
    const int64_t scn_column = storage_schema->get_rowkey_column_num();
    blocksstable::ObDatumRow row;
    ASSERT_EQ(OB_SUCCESS, row.init(allocator_, scn_column + 1));
    const int64_t row_scns[] = {150, 200, 250};
    for (const int64_t row_scn : row_scns) {
      row.storage_datums_[scn_column].set_int(-row_scn);
      ObICompactionFilter::ObFilterRet filter_ret = ObICompactionFilter::FILTER_RET_MAX;
      ASSERT_EQ(OB_SUCCESS, merge_ctx.get_compaction_filter()->filter(row, filter_ret));
      EXPECT_EQ(row_scn <= 200 ? ObICompactionFilter::FILTER_RET_REMOVE
                              : ObICompactionFilter::FILTER_RET_KEEP, filter_ret);
    }
  }
}

TEST_F(TestObMediumCompactionInfoMdsHelperMdsRegister, test_deletion_without_persisted_gc_keeps_rows)
{
  // A Medium scheduled without a safe GC boundary, and Meta Major without a
  // Medium plan, must not introduce a filter as local sibling progress changes.
  const ObMergeType merge_types[] = {MEDIUM_MERGE, META_MAJOR_MERGE};
  for (const ObMergeType merge_type : merge_types) {
    for (int64_t attempt = 0; attempt < 2; ++attempt) {
      ObTabletMergeDagParam param;
      param.merge_type_ = merge_type;
      param.merge_version_ = 300;
      ObTabletMergeCtx merge_ctx(param, allocator_);
      ObStorageSchema *storage_schema = OB_NEWx(ObStorageSchema, &allocator_);
      ASSERT_NE(nullptr, storage_schema);
      merge_ctx.static_param_.schema_ = storage_schema;
      storage_schema->index_type_ = schema::INDEX_TYPE_ASYNC_DELETE_INDEX_LOCAL;
      ASSERT_EQ(OB_SUCCESS, merge_ctx.alloc_mds_info_compaction_filter());
      EXPECT_EQ(nullptr, merge_ctx.get_compaction_filter());
    }
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_medium_compaction_info_mds_helper.log*");
  OB_LOGGER.set_file_name("test_medium_compaction_info_mds_helper.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

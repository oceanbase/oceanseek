/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#include <gtest/gtest.h>
#include "rpc/obrpc/ob_poc_rpc_server.h"
#include "share/ob_cluster_version.h"
#include "storage/lob/access/ob_lob_remote.h"
#include "storage/lob/common/ob_lob_locator.h"
#include "storage/lob/ob_lob_handler.h"
#include "storage/lob/store/ob_lob_aux_manager.h"

namespace oceanbase
{
using namespace common;
using namespace storage;
namespace unittest
{
class TestLobVersionCompat : public ::testing::Test
{
protected:
  void SetUp() override { old_version_ = GET_MIN_CLUSTER_VERSION(); }
  void TearDown() override
  {
    ObClusterVersion::get_instance().update_cluster_version(old_version_);
  }
  uint64_t old_version_;
};

class VersionQueryHandler : public ObLobQueryIterHandler
{
public:
  explicit VersionQueryHandler(ObLobReadContext &ctx) : ObLobQueryIterHandler(ctx) {}
  using ObLobQueryIterHandler::do_execute;
};

TEST_F(TestLobVersionCompat, summary_rpc_available_before_external_lob)
{
  ObLobReadContext ctx;
  ObLobRemoteQueryHandler handler;
  ObAddr addr;
  const uint64_t versions[] = {
      CLUSTER_VERSION_4_6_2_2, CLUSTER_VERSION_4_6_3_0, CLUSTER_VERSION_4_6_3_1};
  EXPECT_EQ(3, static_cast<int>(ObLobQueryArg::GET_LOB_SUMMARY_INFO));
  // Disable forwarding to stop before network IO; verify common summary bypasses the External gate.
  ctx.from_rpc_ = true;
  ctx.enable_remote_retry_ = false;
  for (const uint64_t version : versions) {
    ObClusterVersion::get_instance().update_cluster_version(version);
    EXPECT_EQ(OB_NOT_MASTER,
        ObLobRemoteUtil::query(ctx, ObLobQueryArg::GET_LOB_SUMMARY_INFO, addr, handler));
    EXPECT_EQ(OB_NOT_MASTER, ObLobRemoteUtil::query(ctx, ObLobQueryArg::READ, addr, handler));
    EXPECT_EQ(OB_NOT_MASTER,
        ObLobRemoteUtil::query(ctx, ObLobQueryArg::GET_BYTE_LENGTH, addr, handler));
    EXPECT_EQ(OB_NOT_MASTER,
        ObLobRemoteUtil::query(ctx, ObLobQueryArg::GET_CHAR_LENGTH, addr, handler));
  }
}

TEST_F(TestLobVersionCompat, mixed_cluster_summary_uses_common_remote_path)
{
  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_6_2_2);
  ObArenaAllocator allocator("LobVersionTest");
  ObPersistentLobAdaptor adaptor(OB_SERVER_TENANT_ID);
  ObLobMetaManager meta_mgr;
  ObLobReadContext ctx;
  ObLobMetaInfo summary;
  ctx.allocator_ = &allocator;
  ctx.timeout_ = 0;
  ASSERT_TRUE(ctx.addr_.set_ip_addr("192.0.2.1", 2882));
  ASSERT_EQ(OB_SUCCESS, meta_mgr.init(&adaptor));
  ObLobQueryInfoHandler handler(ctx, summary);
  ASSERT_EQ(OB_SUCCESS, handler.init(&meta_mgr));
  // Exercise the common handler and meta manager; RPC times out before sending.
  EXPECT_EQ(OB_TIMEOUT, handler.execute());
}

TEST_F(TestLobVersionCompat, locator_dispatch_preserves_version_guard)
{
  ObArenaAllocator allocator("LobVersionTest");
  ObLobDiskLocatorBuilder disk_builder;
  ObLobLocatorV2 disk_locator;
  ObString disk;
  ObLobId lob_id;
  lob_id.assign(1, 1001, 2002);
  ASSERT_EQ(OB_SUCCESS, disk_builder.init_outrow(allocator));
  ASSERT_EQ(OB_SUCCESS, disk_builder.set_lob_id(lob_id));
  ASSERT_EQ(OB_SUCCESS, disk_builder.set_seq_no(transaction::ObTxSEQ(10, 0)));
  ASSERT_EQ(OB_SUCCESS, disk_builder.to_locator(disk_locator));
  ASSERT_EQ(OB_SUCCESS, disk_locator.get_disk_locator(disk));

  ObMemLobLocatorBuilder mem_builder;
  ObLobLocatorV2 mem_locator;
  ASSERT_EQ(OB_SUCCESS, mem_builder.init(allocator, disk, false));
  ASSERT_EQ(OB_SUCCESS, mem_builder.build(mem_locator));
  ObExternalBlobInfo info;
  info.url_ = ObString::make_string("s3://bucket/object");
  info.length_ = 1;
  info.kind_ = ObExternalBlobKind::REFERENCE;
  ObMemLobLocatorBuilder external_builder;
  ObLobLocatorV2 external_locator;
  ASSERT_EQ(OB_SUCCESS, external_builder.init(allocator, disk, false));
  ASSERT_EQ(OB_SUCCESS, external_builder.set_extern_info(200, 3, CS_TYPE_BINARY));
  ASSERT_EQ(OB_SUCCESS, external_builder.set_external_lob_info(info));
  ASSERT_EQ(OB_SUCCESS, external_builder.build(external_locator));

  ObLobReadContext ctx;
  ObLobMetaManager meta_mgr;
  ctx.allocator_ = &allocator;
  ctx.timeout_ = 0;
  ASSERT_TRUE(ctx.addr_.set_ip_addr("192.0.2.1", 2882));
  VersionQueryHandler handler(ctx);
  ASSERT_EQ(OB_SUCCESS, handler.init(&meta_mgr));
  // Ordinary Mem goes directly to READ without extra summary queries for External support.
  // READ times out before sending; unexpected summary queries fail with uninitialized meta manager.
  const uint64_t versions[] = {CLUSTER_VERSION_4_6_3_0, CLUSTER_VERSION_4_6_3_1};
  for (const uint64_t version : versions) {
    ObClusterVersion::get_instance().update_cluster_version(version);
    ctx.src_loc_ = mem_locator;
    EXPECT_EQ(OB_TIMEOUT, handler.do_execute());
    EXPECT_EQ(nullptr, handler.result_);
  }

  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_6_3_0);
  ctx.src_loc_ = disk_locator;
  EXPECT_EQ(OB_TIMEOUT, handler.do_execute());
  EXPECT_EQ(nullptr, handler.result_);
  ctx.src_loc_ = external_locator;
  EXPECT_EQ(OB_ERR_UNEXPECTED, handler.do_execute());
  EXPECT_EQ(nullptr, handler.result_);

  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_6_3_1);
  // Bare Disk also goes directly to READ in the new version.
  ctx.src_loc_ = disk_locator;
  EXPECT_EQ(OB_TIMEOUT, handler.do_execute());
  EXPECT_EQ(nullptr, handler.result_);
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  // Handler cleanup needs PNIO even when the query returns before sending.
  int ret = oceanbase::obrpc::global_poc_server.start_net_client(1);
  if (oceanbase::common::OB_SUCCESS == ret) {
    ret = RUN_ALL_TESTS();
  }
  oceanbase::obrpc::global_poc_server.destroy();
  return ret;
}
